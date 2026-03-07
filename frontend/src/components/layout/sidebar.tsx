"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Database,
  Layers,
  GitCompareArrows,
  PieChart,
  Briefcase,
  Settings,
  ChevronDown,
  ShieldAlert,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useState } from "react";

interface NavItem {
  label: string;
  href: string;
  icon: React.ElementType;
  children?: { label: string; href: string; icon: React.ElementType }[];
}

const navItems: NavItem[] = [
  { label: "概览", href: "/", icon: LayoutDashboard },
  { label: "基金数据库", href: "/fund-database", icon: Database },
  {
    label: "基金研究",
    href: "/fund-research",
    icon: PieChart,
    children: [
      { label: "基金池", href: "/fund-research/pools", icon: Layers },
      { label: "基金比较", href: "/fund-research/comparison", icon: GitCompareArrows },
      { label: "组合研究", href: "/fund-research/portfolio", icon: PieChart },
    ],
  },
  { label: "产品运营", href: "/product-ops", icon: Briefcase },
  { label: "风险预警", href: "/risk-alerts", icon: ShieldAlert },
  { label: "系统设置", href: "/settings", icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();
  const [expanded, setExpanded] = useState<string[]>(["基金研究"]);

  function toggle(label: string) {
    setExpanded((p) => (p.includes(label) ? p.filter((l) => l !== label) : [...p, label]));
  }

  function isActive(href: string) {
    return href === "/" ? pathname === "/" : pathname.startsWith(href);
  }

  return (
    <aside className="flex h-screen w-48 flex-col bg-sidebar text-sidebar-foreground shrink-0">
      {/* Brand */}
      <div className="flex items-center gap-2 px-4 h-11 border-b border-sidebar-border">
        <div className="w-5 h-5 rounded bg-jf-gold/90 flex items-center justify-center">
          <span className="text-[10px] font-bold text-white">JF</span>
        </div>
        <span className="text-sm font-semibold tracking-wide">晋帆投研</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-2 px-2 space-y-0.5 overflow-y-auto">
        {navItems.map((item) => {
          const hasChildren = !!item.children?.length;
          const isExp = expanded.includes(item.label);
          const active = isActive(item.href);

          if (hasChildren) {
            return (
              <div key={item.label}>
                <button
                  onClick={() => toggle(item.label)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-[13px] transition-colors",
                    "hover:bg-sidebar-accent",
                    active && "text-sidebar-primary"
                  )}
                >
                  <item.icon className="h-4 w-4 shrink-0 opacity-70" />
                  <span className="flex-1 text-left">{item.label}</span>
                  <ChevronDown className={cn("h-3 w-3 opacity-50 transition-transform", isExp && "rotate-180")} />
                </button>
                {isExp && (
                  <div className="ml-4 mt-0.5 space-y-0.5 border-l border-sidebar-border pl-2">
                    {item.children!.map((child) => (
                      <Link
                        key={child.href}
                        href={child.href}
                        className={cn(
                          "flex items-center gap-2 rounded px-2 py-1.5 text-[12px] transition-colors",
                          "hover:bg-sidebar-accent",
                          isActive(child.href)
                            ? "bg-sidebar-accent text-sidebar-primary font-medium"
                            : "text-sidebar-foreground/60"
                        )}
                      >
                        <child.icon className="h-3.5 w-3.5 shrink-0" />
                        <span>{child.label}</span>
                      </Link>
                    ))}
                  </div>
                )}
              </div>
            );
          }

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-2 rounded px-2 py-1.5 text-[13px] transition-colors",
                "hover:bg-sidebar-accent",
                active
                  ? "bg-sidebar-accent text-sidebar-primary font-medium"
                  : "text-sidebar-foreground/70"
              )}
            >
              <item.icon className="h-4 w-4 shrink-0 opacity-70" />
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      <div className="px-4 py-2 border-t border-sidebar-border">
        <p className="text-[10px] text-sidebar-foreground/30">v0.1.0</p>
      </div>
    </aside>
  );
}
