import type { Metadata } from "next";
import { AppShell } from "@/components/AppShell";
import "./globals.css";

export const metadata: Metadata = {
  title: "TrashPanda — Clean Your Data",
  description:
    "Email database recovery and optimization. Drop a CSV, get back a deliverable list.",
  icons: {
    icon: "/trashpanda-logo.png",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
