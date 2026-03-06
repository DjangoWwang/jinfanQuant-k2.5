"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import {
  ArrowLeft, Loader2, Upload, FileSpreadsheet, ChevronDown, ChevronRight,
  Calendar, Building2, DollarSign, TrendingUp, BarChart3, Eye,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { fetchApi } from "@/lib/api";

const ReactECharts = dynamic(() => import("echarts-for-react"), { ssr: false });

/* --- Types --- */

interface Product {
  id: number;
  product_name: string;
  product_code: string | null;
  custodian: string | null;
  administrator: string | null;
  product_type: string;
  inception_date: string | null;
  total_shares: number | null;
  management_fee_rate: number;
  performance_fee_rate: number;
  high_watermark: number | null;
  notes: string | null;
  latest_nav: number | null;
  latest_total_nav: number | null;
  latest_valuation_date: string | null;
  snapshot_count: number;
}

interface NavPoint {
  date: string;
  unit_nav: number | null;
  total_nav: number | null;
}

interface HoldingItem {
  item_code: string;
  item_name: string;
  level: number;
  parent_code: string | null;
  quantity: number | null;
  unit_cost: number | null;
  cost_amount: number | null;
  cost_pct_nav: number | null;
  market_price: number | null;
  market_value: number | null;
  value_pct_nav: number | null;
  value_diff: number | null;
  linked_fund_id: number | null;
  linked_fund_name: string | null;
}

interface SubFund {
  filing_number: string;
  fund_name: string;
  market_value: number | null;
  weight_pct: number | null;
  appreciation: number | null;
  linked_fund_id: number | null;
}

interface ValuationSnapshot {
  id: number;
  product_id: number;
  valuation_date: string;
  unit_nav: number | null;
  total_nav: number | null;
  total_shares: number | null;
  source_file: string | null;
  imported_at: string | null;
  items: HoldingItem[];
  sub_fund_allocations: SubFund[];
}

interface UploadResult {
  snapshot_id: number | null;
  product_id: number;
  file_name: string;
  valuation_date: string | null;
  unit_nav: number | null;
  total_nav: number | null;
  holdings_count: number;
  sub_funds_count: number;
  sub_funds_linked: number;
  warnings: string[];
}

/* --- Page --- */

export default function ProductDetailPage() {
  const params = useParams();
  const router = useRouter();
  const productId = Number(params.productId);

  const [product, setProduct] = useState<Product | null>(null);
  const [navSeries, setNavSeries] = useState<NavPoint[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<ValuationSnapshot | null>(null);
  const [allSnapshots, setAllSnapshots] = useState<ValuationSnapshot[]>([]);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [expandedL1, setExpandedL1] = useState<Set<string>>(new Set());
  const [expandedL2, setExpandedL2] = useState<Set<string>>(new Set());
  const fileInputRef = useRef<HTMLInputElement>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [prod, nav, valuations] = await Promise.all([
        fetchApi<Product>(`/products/${productId}`),
        fetchApi<{ nav_series: NavPoint[] }>(`/products/${productId}/nav`),
        fetchApi<{ items: ValuationSnapshot[]; total: number }>(`/products/${productId}/valuations?page_size=50`),
      ]);
      setProduct(prod);
      setNavSeries(nav.nav_series);
      setAllSnapshots(valuations.items);

      // Load latest snapshot detail
      if (valuations.items.length > 0) {
        const latest = await fetchApi<ValuationSnapshot>(
          `/products/${productId}/valuation/${valuations.items[0].id}`
        );
        setLatestSnapshot(latest);
      }
    } catch (e) {
      console.error("Load failed:", e);
    } finally {
      setLoading(false);
    }
  }, [productId]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  async function handleUpload(file: File) {
    setUploading(true);
    setUploadResult(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1"}/products/${productId}/valuation`,
        { method: "POST", body: formData }
      );
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `Upload failed: ${res.status}`);
      }
      const result: UploadResult = await res.json();
      setUploadResult(result);
      await loadData();
    } catch (e: any) {
      setUploadResult({
        snapshot_id: null,
        product_id: productId,
        file_name: file.name,
        valuation_date: null,
        unit_nav: null,
        total_nav: null,
        holdings_count: 0,
        sub_funds_count: 0,
        sub_funds_linked: 0,
        warnings: [`上传失败: ${e.message}`],
      });
    } finally {
      setUploading(false);
    }
  }

  function toggleL1(code: string) {
    setExpandedL1((prev) => {
      const next = new Set(prev);
      next.has(code) ? next.delete(code) : next.add(code);
      return next;
    });
  }

  function toggleL2(code: string) {
    setExpandedL2((prev) => {
      const next = new Set(prev);
      next.has(code) ? next.delete(code) : next.add(code);
      return next;
    });
  }

  if (loading || !product) {
    return (
      <div className="h-96 flex items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  /* --- NAV Chart --- */
  const navChartOption = {
    tooltip: {
      trigger: "axis",
      formatter: (params: any) => {
        const p = params[0];
        return `${p.axisValue}<br/>单位净值: <b>${p.value?.toFixed(4) ?? "--"}</b>`;
      },
    },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: {
      type: "category",
      data: navSeries.map((n) => n.date),
      axisLabel: { fontSize: 10 },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { fontSize: 10 },
    },
    series: [
      {
        name: "单位净值",
        type: "line",
        data: navSeries.map((n) => n.unit_nav),
        smooth: true,
        symbol: "none",
        lineStyle: { width: 2 },
        areaStyle: {
          color: {
            type: "linear",
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(59,130,246,0.15)" },
              { offset: 1, color: "rgba(59,130,246,0.01)" },
            ],
          },
        },
      },
    ],
  };

  /* --- Sub-fund Pie Chart --- */
  const subFunds = latestSnapshot?.sub_fund_allocations || [];
  const pieData = subFunds
    .filter((s) => s.weight_pct != null && s.weight_pct > 0)
    .map((s) => ({ name: s.fund_name, value: s.weight_pct }));

  const pieChartOption = {
    tooltip: {
      trigger: "item",
      formatter: (p: any) => `${p.name}<br/>占比: <b>${p.value?.toFixed(2)}%</b>`,
    },
    series: [
      {
        type: "pie",
        radius: ["35%", "65%"],
        label: { fontSize: 10, formatter: "{b}: {d}%" },
        data: pieData,
      },
    ],
  };

  /* --- Holding hierarchy --- */
  const holdings = latestSnapshot?.items || [];
  const l1Items = holdings.filter((h) => h.level === 1);

  function getChildren(parentCode: string, level: number) {
    return holdings.filter(
      (h) => h.level === level && h.parent_code === parentCode
    );
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title={product.product_name}
        breadcrumb={["产品运营"]}
        actions={
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" className="h-8 text-[12px] gap-1" onClick={() => router.push("/product-ops")}>
              <ArrowLeft className="h-3 w-3" />返回
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleUpload(file);
                e.target.value = "";
              }}
            />
            <Button
              size="sm"
              className="h-8 text-[12px] gap-1.5"
              disabled={uploading}
              onClick={() => fileInputRef.current?.click()}
            >
              {uploading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
              上传估值表
            </Button>
          </div>
        }
      />

      {/* Upload result toast */}
      {uploadResult && (
        <div className={`p-3 rounded border text-[12px] ${
          uploadResult.warnings.some((w) => w.startsWith("上传失败"))
            ? "bg-red-50 border-red-200 text-red-700"
            : "bg-green-50 border-green-200 text-green-700"
        }`}>
          <p className="font-medium">{uploadResult.file_name}</p>
          {uploadResult.valuation_date && <p>估值日期: {uploadResult.valuation_date}</p>}
          {uploadResult.unit_nav != null && <p>单位净值: {uploadResult.unit_nav.toFixed(4)}</p>}
          <p>持仓明细: {uploadResult.holdings_count} 条 | 子基金: {uploadResult.sub_funds_count} 只 | 已关联: {uploadResult.sub_funds_linked} 只</p>
          {uploadResult.warnings.map((w, i) => (
            <p key={i} className="text-amber-600 mt-0.5">{w}</p>
          ))}
          <button className="text-[11px] underline mt-1" onClick={() => setUploadResult(null)}>关闭</button>
        </div>
      )}

      {/* Product info cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <InfoCard icon={DollarSign} label="最新净值" value={product.latest_nav?.toFixed(4) ?? "--"} sub={product.latest_valuation_date ?? ""} />
        <InfoCard icon={BarChart3} label="资产净值" value={fmtMoney(product.latest_total_nav)} sub="总规模" />
        <InfoCard icon={Calendar} label="成立日" value={product.inception_date ?? "--"} sub={product.product_type === "live" ? "实盘" : "模拟"} />
        <InfoCard icon={Building2} label="托管/管理" value={product.custodian ?? "--"} sub={`费率 ${product.management_fee_rate}%/${product.performance_fee_rate}%`} />
      </div>

      {/* Main tabs */}
      <Tabs defaultValue="overview">
        <TabsList className="h-8">
          <TabsTrigger value="overview" className="text-[12px] h-7 px-3">概览</TabsTrigger>
          <TabsTrigger value="holdings" className="text-[12px] h-7 px-3">持仓明细</TabsTrigger>
          <TabsTrigger value="history" className="text-[12px] h-7 px-3">估值历史</TabsTrigger>
        </TabsList>

        {/* Overview tab */}
        <TabsContent value="overview" className="mt-3 space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {/* NAV chart */}
            <div className="lg:col-span-2 bg-card border border-border rounded p-3">
              <h3 className="text-[13px] font-medium mb-2">净值走势</h3>
              {navSeries.length > 0 ? (
                <ReactECharts option={navChartOption} style={{ height: 280 }} />
              ) : (
                <div className="h-[280px] flex items-center justify-center text-muted-foreground text-[12px]">
                  暂无净值数据，请上传估值表
                </div>
              )}
            </div>

            {/* Sub-fund allocation pie */}
            <div className="bg-card border border-border rounded p-3">
              <h3 className="text-[13px] font-medium mb-2">子基金配置</h3>
              {pieData.length > 0 ? (
                <ReactECharts option={pieChartOption} style={{ height: 280 }} />
              ) : (
                <div className="h-[280px] flex items-center justify-center text-muted-foreground text-[12px]">
                  暂无持仓数据
                </div>
              )}
            </div>
          </div>

          {/* Sub-fund table */}
          {subFunds.length > 0 && (
            <div className="bg-card border border-border rounded overflow-hidden">
              <h3 className="text-[13px] font-medium p-3 pb-2">子基金持仓</h3>
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal">基金名称</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">备案号</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">占比</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">浮盈</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">关联</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {subFunds.map((sf, i) => (
                    <TableRow key={i} className="text-[12px]">
                      <TableCell className="py-2 font-medium">{sf.fund_name}</TableCell>
                      <TableCell className="py-2 text-center text-muted-foreground text-[11px]">{sf.filing_number}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtMoney(sf.market_value)}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtPct(sf.weight_pct)}</TableCell>
                      <TableCell className={`py-2 text-right tabular-nums ${
                        (sf.appreciation ?? 0) >= 0 ? "text-green-600" : "text-red-600"
                      }`}>
                        {fmtMoney(sf.appreciation)}
                      </TableCell>
                      <TableCell className="py-2 text-center">
                        {sf.linked_fund_id ? (
                          <button
                            className="text-[11px] text-primary hover:underline"
                            onClick={() => router.push(`/fund-database/${sf.linked_fund_id}`)}
                          >
                            <Eye className="h-3 w-3 inline mr-0.5" />查看
                          </button>
                        ) : (
                          <span className="text-[11px] text-muted-foreground">未关联</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>

        {/* Holdings tab — hierarchical table */}
        <TabsContent value="holdings" className="mt-3">
          <div className="bg-card border border-border rounded overflow-hidden">
            {holdings.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-muted-foreground text-[12px]">
                暂无持仓明细，请先上传估值表
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal">科目代码</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">科目名称</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">数量</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">成本</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">占净值%</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">估值增值</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {l1Items.map((l1) => {
                    const isExpL1 = expandedL1.has(l1.item_code);
                    const children2 = getChildren(l1.item_code, 2);
                    const hasChildren = children2.length > 0;

                    return (
                      <HoldingRows
                        key={l1.item_code}
                        item={l1}
                        level={1}
                        expanded={isExpL1}
                        hasChildren={hasChildren}
                        onToggle={() => toggleL1(l1.item_code)}
                      >
                        {isExpL1 && children2.map((l2) => {
                          const isExpL2 = expandedL2.has(l2.item_code);
                          const children3 = getChildren(l2.item_code, 3);
                          const children4 = getChildren(l2.item_code, 4);
                          const subItems = [...children3, ...children4];
                          const hasSubChildren = subItems.length > 0;

                          return (
                            <HoldingRows
                              key={l2.item_code}
                              item={l2}
                              level={2}
                              expanded={isExpL2}
                              hasChildren={hasSubChildren}
                              onToggle={() => toggleL2(l2.item_code)}
                            >
                              {isExpL2 && subItems.map((l34) => (
                                <HoldingRows
                                  key={l34.item_code}
                                  item={l34}
                                  level={l34.level}
                                  expanded={false}
                                  hasChildren={false}
                                  onToggle={() => {}}
                                />
                              ))}
                            </HoldingRows>
                          );
                        })}
                      </HoldingRows>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>

        {/* Valuation history tab */}
        <TabsContent value="history" className="mt-3">
          <div className="bg-card border border-border rounded overflow-hidden">
            {allSnapshots.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-muted-foreground text-[12px]">
                暂无估值记录
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal w-12 text-center">序号</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">估值日期</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">单位净值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-right">资产净值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">来源文件</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">导入时间</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {allSnapshots.map((s, i) => (
                    <TableRow key={s.id} className="text-[12px]">
                      <TableCell className="py-2 text-center text-muted-foreground">{i + 1}</TableCell>
                      <TableCell className="py-2 tabular-nums font-medium">{s.valuation_date}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{s.unit_nav?.toFixed(4) ?? "--"}</TableCell>
                      <TableCell className="py-2 text-right tabular-nums">{fmtMoney(s.total_nav)}</TableCell>
                      <TableCell className="py-2 text-muted-foreground text-[11px] max-w-[200px] truncate">
                        {s.source_file || "--"}
                      </TableCell>
                      <TableCell className="py-2 text-muted-foreground text-[11px] tabular-nums">
                        {s.imported_at ? s.imported_at.slice(0, 16).replace("T", " ") : "--"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>
      </Tabs>

      {/* Product notes */}
      {product.notes && (
        <div className="bg-card border border-border rounded p-3">
          <h3 className="text-[13px] font-medium mb-1">备注</h3>
          <p className="text-[12px] text-muted-foreground whitespace-pre-wrap">{product.notes}</p>
        </div>
      )}
    </div>
  );
}

/* --- InfoCard --- */

function InfoCard({ icon: Icon, label, value, sub }: {
  icon: React.ElementType;
  label: string;
  value: string;
  sub: string;
}) {
  return (
    <div className="bg-card border border-border rounded p-3">
      <div className="flex items-center gap-1.5 mb-1">
        <Icon className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-[11px] text-muted-foreground">{label}</span>
      </div>
      <p className="text-[16px] font-semibold tabular-nums">{value}</p>
      <p className="text-[10px] text-muted-foreground mt-0.5">{sub}</p>
    </div>
  );
}

/* --- HoldingRows (recursive hierarchical table row) --- */

function HoldingRows({ item, level, expanded, hasChildren, onToggle, children }: {
  item: HoldingItem;
  level: number;
  expanded: boolean;
  hasChildren: boolean;
  onToggle: () => void;
  children?: React.ReactNode;
}) {
  const indent = (level - 1) * 16;
  const bgClass = level === 1 ? "bg-muted/20 font-medium" : level === 2 ? "bg-muted/10" : "";

  return (
    <>
      <TableRow className={`text-[12px] ${bgClass}`}>
        <TableCell className="py-1.5" style={{ paddingLeft: 8 + indent }}>
          <div className="flex items-center gap-1">
            {hasChildren ? (
              <button onClick={onToggle} className="p-0.5 hover:bg-muted rounded">
                {expanded
                  ? <ChevronDown className="h-3 w-3 text-muted-foreground" />
                  : <ChevronRight className="h-3 w-3 text-muted-foreground" />
                }
              </button>
            ) : (
              <span className="w-4" />
            )}
            <span className="text-[11px] text-muted-foreground tabular-nums">{item.item_code}</span>
          </div>
        </TableCell>
        <TableCell className="py-1.5">{item.item_name}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.quantity, 0)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.cost_amount)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtNum(item.market_value)}</TableCell>
        <TableCell className="py-1.5 text-right tabular-nums">{fmtPct(item.value_pct_nav)}</TableCell>
        <TableCell className={`py-1.5 text-right tabular-nums ${
          (item.value_diff ?? 0) >= 0 ? "text-green-600" : "text-red-600"
        }`}>
          {fmtNum(item.value_diff)}
        </TableCell>
      </TableRow>
      {children}
    </>
  );
}

function fmtNum(v: number | null | undefined, dp = 2): string {
  if (v == null || !isFinite(v)) return "--";
  return v.toLocaleString("zh-CN", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  return `${v.toFixed(2)}%`;
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return "--";
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}万`;
  return v.toFixed(2);
}
