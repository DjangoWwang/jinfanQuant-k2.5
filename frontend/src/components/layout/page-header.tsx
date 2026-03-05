"use client";

import { ReactNode } from "react";

interface PageHeaderProps {
  title: string;
  description?: string;
  breadcrumb?: string[];
  actions?: ReactNode;
}

export function PageHeader({ title, description, breadcrumb, actions }: PageHeaderProps) {
  return (
    <div className="flex items-center justify-between pb-3 mb-3 border-b border-border">
      <div className="flex items-center gap-3">
        {breadcrumb && breadcrumb.length > 0 && (
          <div className="flex items-center gap-1 text-[11px] text-muted-foreground">
            {breadcrumb.map((item, i) => (
              <span key={i}>
                {i > 0 && <span className="mx-1 opacity-40">/</span>}
                {item}
              </span>
            ))}
            <span className="mx-1 opacity-40">/</span>
          </div>
        )}
        <h1 className="text-[15px] font-semibold">{title}</h1>
        {description && <span className="text-[12px] text-muted-foreground">{description}</span>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  );
}
