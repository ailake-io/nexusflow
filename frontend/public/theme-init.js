// Applies the saved theme before first paint so a light-theme user doesn't see
// a dark flash while the app bundle loads. External file (not inline) because
// index.html's CSP is `script-src 'self'`. Key must match lib/theme/ThemeProvider.tsx.
try {
  if (localStorage.getItem('nexusflow-theme') === 'light') {
    document.documentElement.classList.add('light')
  }
} catch {
  // storage unavailable: fall back to the default (dark)
}
