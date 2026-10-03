#!/usr/bin/env bash
# Build the real CVC4 1.8 CLI for one-task browser workers.
set -euo pipefail
src="$(cd "$(dirname "$0")" && pwd)"
source "${src}/pins.env"
bundle="${1:?bundle directory}"
work=/tmp/cvc4-web-build
mkdir -p "${work}"
cd "${work}"
fetch() {
  curl -fsSL --retry 3 "$1" -o "$3"
  echo "$2  $3" | sha256sum -c -
}
fetch "${EMSCRIPTEN_URL}" "${EMSCRIPTEN_SHA256}" emscripten.tar.xz
tar -xJf emscripten.tar.xz
export EM_CONFIG="${work}/emscripten.config"
cat > "${EM_CONFIG}" <<EOF
LLVM_ROOT = '${work}/install/bin'
BINARYEN_ROOT = '${work}/install'
NODE_JS = ['$(command -v node)']
EOF
export PATH="${work}/install/emscripten:${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk}/bin:${PATH}"
emcc --version

fetch "${GMP_URL}" "${GMP_SHA256}" gmp.tar.xz
tar -xJf gmp.tar.xz
prefix="${work}/deps"
(
  cd gmp-6.3.0
  emconfigure ./configure --host=wasm32-unknown-emscripten --prefix="${prefix}" \
    --disable-assembly --disable-shared --enable-static --enable-cxx --with-pic
  emmake make -j2
  emmake make install
)
fetch "${ANTLR_JAR_URL}" "${ANTLR_JAR_SHA256}" antlr.jar
fetch "${ANTLR_C_URL}" "${ANTLR_C_SHA256}" antlr.tgz
tar -xzf antlr.tgz
# Match CVC4's own contrib/get-antlr-3.4: unused debug handlers have missing symbols.
: > libantlr3c-3.4/src/antlr3debughandlers.c
(
  cd libantlr3c-3.4
  emconfigure ./configure --host=none --prefix="${prefix}" \
    --disable-shared --enable-static --disable-antlrdebug
  emmake make -j2
  emmake make install
)
mkdir -p "${prefix}/bin"
cat > "${prefix}/bin/antlr3" <<EOF
#!/bin/sh
exec java -cp '${work}/antlr.jar' org.antlr.Tool "\$@"
EOF
chmod +x "${prefix}/bin/antlr3"
export PATH="${prefix}/bin:${PATH}"
fetch "${SYMFPU_URL}" "${SYMFPU_SHA256}" symfpu.tgz
mkdir -p "${prefix}/include/symfpu"
tar -xzf symfpu.tgz --strip-components=1 -C "${prefix}/include/symfpu"

fetch "${CVC4_SOURCE_URL}" "${CVC4_SOURCE_SHA256}" cvc4.tgz
tar -xzf cvc4.tgz
cvc4="${work}/CVC4-archived-${CVC4_VERSION}"
python3 - "${cvc4}/src" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
# Python 3.11+ provides the TOML parser needed by the upstream code generator.
p = root / 'options/mkoptions.py'
s = p.read_text()
assert s.count('import toml\n') == 1 and s.count('toml.load(filename)') == 1
p.write_text(s.replace('import toml\n', 'import tomllib\n')
             .replace('toml.load(filename)', 'tomllib.loads(open(filename).read())'))
p = root / 'options/CMakeLists.txt'
s = p.read_text()
assert s.count('import toml') == 1
p.write_text(s.replace('import toml', 'import tomllib'))
# Bash 5.2+ otherwise interprets C++ ampersands as replacement patterns.
for p in root.rglob('mk*'):
    if p.is_file():
        s = p.read_text()
        if 'eval text=' in s:
            p.write_text(s.replace('#!/usr/bin/env bash\n',
                                   '#!/usr/bin/env bash\nshopt -u patsub_replacement\n', 1))
# Browsers have no POSIX signal stack; cancellation/budgets terminate the worker.
p = root / 'main/util.cpp'
s = p.read_text()
for block in ('  struct rlimit limit;', '#ifdef HAVE_SIGALTSTACK\n  free(cvc4StackBase);'):
    old = '#ifndef __WIN32__\n' + block
    assert s.count(old) == 1
    s = s.replace(old, '#if !defined(__WIN32__) && !defined(__EMSCRIPTEN__)\n' + block)
p.write_text(s)
# libc++'s iosfwd does not implicitly expose the global size_t typedef.
for name in ('expr/emptyset.h', 'expr/expr_iomanip.h', 'util/regexp.h'):
    p = root / name
    s = p.read_text()
    assert s.count('#include <iosfwd>') == 1
    p.write_text(s.replace('#include <iosfwd>', '#include <stddef.h>\n#include <iosfwd>'))
PY
emcmake cmake -S "${cvc4}" -B build -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DCMAKE_FIND_ROOT_PATH="${prefix}" \
  -DCMAKE_BUILD_TYPE=Production -DENABLE_SHARED=OFF -DENABLE_UNIT_TESTING=OFF \
  -DUSE_SYMFPU=ON -DSYMFPU_DIR="${prefix}" -DGMP_DIR="${prefix}" -DANTLR_DIR="${prefix}" \
  -DPYTHON_EXECUTABLE="$(command -v python3)" \
  -DCMAKE_CXX_FLAGS=-fexceptions \
  "-DCMAKE_EXE_LINKER_FLAGS=-sMODULARIZE=1 -sEXPORT_NAME=createCvc4 -sINVOKE_RUN=0 -sALLOW_MEMORY_GROWTH=1 -sSTACK_SIZE=8388608 -sDISABLE_EXCEPTION_CATCHING=0 -sENVIRONMENT=worker,node -sEXPORTED_RUNTIME_METHODS=FS,callMain"
cmake --build build --target cvc4-bin -j4
cp build/bin/cvc4.js build/bin/cvc4.wasm "${bundle}/"
cp "${cvc4}/COPYING" "${bundle}/LICENSES/cvc4.COPYING"
cp -r "${cvc4}/licenses" "${bundle}/LICENSES/cvc4-licenses"
cp gmp-6.3.0/COPYING* "${bundle}/LICENSES/"
cp libantlr3c-3.4/COPYING "${bundle}/LICENSES/antlr3c.COPYING"
cp "${prefix}/include/symfpu/LICENSE" "${bundle}/LICENSES/symfpu.LICENSE"
cp install/emscripten/LICENSE "${bundle}/LICENSES/emscripten.LICENSE"
cp install/emscripten/system/lib/libc/musl/COPYRIGHT "${bundle}/LICENSES/musl.COPYRIGHT"
cp install/emscripten/system/lib/libcxx/LICENSE.TXT "${bundle}/LICENSES/libcxx.LICENSE.TXT"
cp install/emscripten/system/lib/libcxxabi/LICENSE.TXT "${bundle}/LICENSES/libcxxabi.LICENSE.TXT"
