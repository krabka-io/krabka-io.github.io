# krabka.io

The official website and documentation portal for the [Krabka](https://github.com/krabka-io) streaming ecosystem, built with [Astro](https://astro.build), [Starlight](https://starlight.astro.build), and [Tailwind CSS](https://tailwindcss.com).

## 🚀 Quick Start

```bash
# Install dependencies
npm install

# Start local development server with hot reload
npm run dev

# Build production static bundle (generates to ./dist and indexes search with Pagefind)
npm run build

# Preview production build locally
npm run preview
```

## 🏗️ Project Structure

```
krabka-website/
├── public/
│   ├── CNAME                     # Custom domain (krabka.io)
│   ├── favicon.svg               # Cooked Dungeness crab geometric SVG icon
│   └── logo.png                  # Krabka crab brand mark
├── src/
│   ├── components/               # UI components (Hero, Navbar, MetricStrip, CodeSwitcher, EcosystemGrid, BenchmarkBar)
│   ├── content/docs/             # Starlight documentation MDX files
│   ├── layouts/BaseLayout.astro  # Root HTML layout with SEO metadata & Inter font
│   └── pages/                    # Landing, Get Started, Features, Versions, Benchmarks
├── astro.config.mjs              # Astro, Starlight & Tailwind configuration
└── .github/workflows/deploy.yml  # Automated GitHub Pages CI/CD workflow
```

## 📜 License

Apache License 2.0.
