import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    sourcemap: false,
    license: { fileName: "assets/THIRD_PARTY_LICENSES.md" },
  },
  server: { host: "127.0.0.1", port: 5173, proxy: { "/v1": "http://127.0.0.1:8787" } },
});
