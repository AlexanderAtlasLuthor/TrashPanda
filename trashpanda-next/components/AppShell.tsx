"use client";

import { useState, useCallback, createContext, useContext } from "react";
import { Sidebar } from "./Sidebar";
import styles from "./AppShell.module.css";

interface ShellContextValue {
  toggleSidebar: () => void;
}

const ShellContext = createContext<ShellContextValue | null>(null);

/**
 * Child components (like Topbar) use this to open the mobile sidebar
 * without prop drilling.
 */
export function useShell(): ShellContextValue {
  const ctx = useContext(ShellContext);
  if (!ctx) {
    return { toggleSidebar: () => {} };
  }
  return ctx;
}

/**
 * Wraps all app routes. Owns sidebar drawer state for mobile.
 */
export function AppShell({ children }: { children: React.ReactNode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const closeSidebar = useCallback(() => setSidebarOpen(false), []);
  const toggleSidebar = useCallback(() => setSidebarOpen((s) => !s), []);

  return (
    <ShellContext.Provider value={{ toggleSidebar }}>
      <div className={styles.app}>
        <Sidebar open={sidebarOpen} onClose={closeSidebar} />
        <main className={styles.main}>{children}</main>
      </div>
    </ShellContext.Provider>
  );
}
