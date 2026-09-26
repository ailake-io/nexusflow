import path from 'node:path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(import.meta.dirname, './src'),
    },
  },
  server: {
    // nexus-server (Axum) has no CORS layer yet — proxy in dev instead of
    // adding one just to unblock local testing. Production serves the built
    // frontend from the same origin as the API (CLAUDE.md §7), so this is
    // dev-only plumbing.
    proxy: {
      '/auth': 'http://localhost:8080',
      '/connectors': 'http://localhost:8080',
      '/pipelines': { target: 'http://localhost:8080', ws: true },
      '/users': 'http://localhost:8080',
      '/license': 'http://localhost:8080',
      '/health': 'http://localhost:8080',
      '/system': 'http://localhost:8080',
      '/lineage': 'http://localhost:8080',
      '/rag': 'http://localhost:8080',
      '/agents': 'http://localhost:8080',
      // Pre-existing gap, fixed in passing: missing here meant
      // PromptLibrary.tsx's GET/POST /prompts 404'd under `npm run dev`
      // (silently worked in the embed-ui production build, same origin).
      '/prompts': 'http://localhost:8080',
    },
  },
})
