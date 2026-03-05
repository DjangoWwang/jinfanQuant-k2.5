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
  Diamond,
  ChevronDown,
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
  {
    label: "概览",
    href: "/",
    icon: LayoutDashboard,
  },
  {
    label: "基金数据库",
    href: "/fund-database",
    icon: Database,
  },
  {
    label: "基金研究",
    href: "/fund-research",
    icon: PieChart,
    children: [
      { label: "基金池", href: "/fund-research/pools", icon: Layers },
      {
        label: "基金比较",
        href: "/fund-research/comparison",
        icon: GitCompareArrows,
      },
      {
        label: "组合研究",
        href: "/fund-research/portfolio",
        icon: PieChart,
      },
    ],
  },
  {
    label: "产品运营",
    href: "/product-ops",
    icon: Briefcase,
  },
  {
    label: "系统设置",
    href: "/settings",
    icon: Settings,
  },
];

export function Sidebar() {
  const pathname = usePathname();
  const [expandedSections, setExpandedSections] = useState<string[]>([
    "基金研究",
  ]);

  function toggleSection(label: string) {
    setExpandedSections((prev) =>
      prev.includes(label)
        ? prev.filter((l) => l !== label)
        : [...prev, label]
    );
  }

  function isActive(href: string) {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  }

  return (
    <aside className="flex h-screen w-60 flex-col bg-sidebar text-sidebar-foreground">
      {/* Logo */}
      <div className="flex items-center gap-3 px-6 py-6">
        <Diamond className="h-6 w-6 text-jf-gold" />
        <span className="text-lg font-bold tracking-wide">晋帆投研</span>
      </div>

      {/* Divider */}
      <div className="mx-4 h-px bg-sidebar-border" />

      {/* Navigation */}
      <nav className="mt-4 flex flex-1 flex-col gap-1 px-3">
        {navItems.map((item) => {
          const hasChildren = item.children && item.children.length > 0;
          const expanded = expandedSections.includes(item.label);
          const active = isActive(item.href);

          if (hasChildren) {
            return (
              <div key={item.label}>
                <button
                  onClick={() => toggleSection(item.label)}
                  className={cn(
                    "flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                    "hover:bg-sidebar-accent",
                    active && "text-jf-gold"
                  )}
                >
                  <item.icon className="h-[18px] w-[18px] shrink-0" />
                  <span className="flex-1 text-left">{item.label}</span>
                  <ChevronDown
                    className={cn(
                      "h-4 w-4 transition-transform duration-200",
                      expanded && "rotate-180"
                    )}
                  />
                </button>
                {expanded && (
                  <div className="ml-3 mt-1 flex flex-col gap-0.5 border-l border-sidebar-border pl-3">
                    {item.children!.map((child) => {
                      const childActive = isActive(child.href);
                      return (
                        <Link
                          key={child.href}
                          href={child.href}
                          className={cn(
                            "flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors",
                            "hover:bg-sidebar-accent",
                            childActive
                              ? "border-l-2 border-jf-gold bg-sidebar-accent text-jf-gold -ml-[13px] pl-[22px]"
                              : "text-sidebar-foreground/70"
                          )}
                        >
                          <child.icon className="h-4 w-4 shrink-0" />
                          <span>{child.label}</span>
                        </Link>
                      );
                    })}
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
                "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                "hover:bg-sidebar-accent",
                active
                  ? "border-l-2 border-jf-gold bg-sidebar-accent text-jf-gold"
                  : "text-sidebar-foreground/80"
              )}
            >
              <item.icon className="h-[18px] w-[18px] shrink-0" />
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      {/* Bottom */}
      <div className="px-6 py-4">
        <div className="h-px bg-sidebar-border" />
        <p className="mt-3 text-xs text-sidebar-foreground/40">v0.1.0</p>
      </div>
    </aside>
  );
}
