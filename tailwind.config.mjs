/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        krab: {
          50: '#fff5f2',
          100: '#ffe8e1',
          200: '#ffd4c7',
          300: '#ffb39e',
          400: '#ff8466',
          500: '#ff4d2e', // Cooked Dungeness Vermilion
          600: '#f03211',
          700: '#c82408',
          800: '#a4210b',
          900: '#872110',
          950: '#4a0c03',
        },
        ocean: {
          700: '#1e293b',
          800: '#0f172a',
          900: '#0c1322', // Deep Pacific Ocean Navy
          950: '#080d1a', // Darkest Ocean Abyss
        }
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'Monaco', 'Consolas', 'monospace'],
      },
    },
  },
  plugins: [],
};
