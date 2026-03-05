export interface Fund {
  id: number;
  fund_name: string;
  filing_number: string;
  manager_name: string;
  strategy_type: string;
  strategy_sub: string | null;
  nav_frequency: "daily" | "weekly";
  latest_nav: number | null;
  inception_date: string;
  is_private: boolean;
}
