import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Proxy to Python backend in dev (adjust URL when backend is running)
  async rewrites() {
    const backend = process.env.TRASHPANDA_BACKEND_URL;
    if (!backend) return [];
    return [
      {
        source: "/api/backend/:path*",
        destination: `${backend}/:path*`,
      },
    ];
  },
};

export default nextConfig;
