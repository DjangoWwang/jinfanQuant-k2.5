"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import {
  Plus, Loader2, Briefcase, ChevronRight, Upload,
  TrendingUp, TrendingDown, Minus,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { fetchApi } from "@/lib/api";

/* --- Types --- */

interface ProductItem {
  id: number;
  product_name: string;
  product_code: string | null;
  custodian: string | null;
  administrator: string | null;
  product_type: string;
  inception_date: string | null;
  management_fee_rate: number;
  performance_fee_rate: number;
  latest_nav: number | null;
  latest_total_nav: number | null;
  latest_valuation_date: string | null;
  snapshot_count: number;
  is_active: boolean;
}

/* --- Page --- */

export default function ProductOpsPage() {
  const router = useRouter();
  const [products, setProducts] = useState<ProductItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState("all");
  const [showCreate, setShowCreate] = useState(false);
  const [creating, setCreating] = useState(false);

  // Create form
  const [formName, setFormName] = useState("");
  const [formCode, setFormCode] = useState("");
  const [formCustodian, setFormCustodian] = useState("");
  const [formType, setFormType] = useState("live");
  const [formInceptionDate, setFormInceptionDate] = useState("");

  const loadProducts = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetchApi<{ items: ProductItem[]; total: number }>("/products/");
      setProducts(res.items);
    } catch {
      setProducts([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadProducts();
  }, [loadProducts]);

  const filtered = tab === "all"
    ? products
    : products.filter((p) => p.product_type === tab);

  const liveCount = products.filter((p) => p.product_type === "live").length;
  const simCount = products.filter((p) => p.product_type === "simulation").length;

  async function handleCreate() {
    if (!formName.trim()) return;
    setCreating(true);
    try {
      await fetchApi("/products/", {
        method: "POST",
        body: JSON.stringify({
          product_name: formName.trim(),
          product_code: formCode.trim() || null,
          custodian: formCustodian.trim() || null,
          product_type: formType,
          inception_date: formInceptionDate || null,
        }),
      });
      setShowCreate(false);
      setFormName("");
      setFormCode("");
      setFormCustodian("");
      setFormType("live");
      setFormInceptionDate("");
      await loadProducts();
    } finally {
      setCreating(false);
    }
  }

  function NavChange({ nav }: { nav: number | null }) {
    if (nav === null) return <span className="text-muted-foreground">--</span>;
    return <span className="tabular-nums font-medium">{nav.toFixed(4)}</span>;
  }

  return (
    <div className="space-y-4">
      <PageHeader
        title="产品运营"
        description="管理实盘与模拟FOF产品、估值表导入"
        actions={
          <Button
            size="sm"
            className="h-8 text-[12px] gap-1.5 px-4"
            onClick={() => setShowCreate(true)}
          >
            <Plus className="h-3.5 w-3.5" />新建产品
          </Button>
        }
      />

      {/* Stats cards */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-card border border-border rounded p-3">
          <p className="text-[11px] text-muted-foreground">产品总数</p>
          <p className="text-xl font-semibold tabular-nums mt-0.5">{products.length}</p>
        </div>
        <div className="bg-card border border-border rounded p-3">
          <p className="text-[11px] text-muted-foreground">实盘产品</p>
          <p className="text-xl font-semibold tabular-nums mt-0.5 text-green-600">{liveCount}</p>
        </div>
        <div className="bg-card border border-border rounded p-3">
          <p className="text-[11px] text-muted-foreground">模拟产品</p>
          <p className="text-xl font-semibold tabular-nums mt-0.5 text-blue-600">{simCount}</p>
        </div>
      </div>

      {/* Tabs + Table */}
      <Tabs value={tab} onValueChange={setTab}>
        <TabsList className="h-8">
          <TabsTrigger value="all" className="text-[12px] h-7 px-3">全部 ({products.length})</TabsTrigger>
          <TabsTrigger value="live" className="text-[12px] h-7 px-3">实盘 ({liveCount})</TabsTrigger>
          <TabsTrigger value="simulation" className="text-[12px] h-7 px-3">模拟 ({simCount})</TabsTrigger>
        </TabsList>

        <TabsContent value={tab} className="mt-3">
          <div className="bg-card border border-border rounded overflow-hidden">
            {loading ? (
              <div className="h-40 flex items-center justify-center">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : filtered.length === 0 ? (
              <div className="h-48 flex flex-col items-center justify-center text-muted-foreground gap-3">
                <Briefcase className="h-10 w-10 opacity-20" />
                <div className="text-center">
                  <p className="text-[13px] mb-1">暂无产品</p>
                  <p className="text-[11px] opacity-60">点击"新建产品"创建实盘或模拟FOF产品</p>
                </div>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/40">
                    <TableHead className="h-8 text-[11px] font-normal w-12 text-center">序号</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal">产品名称</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">类型</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">托管人</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">最新净值</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">估值日期</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">估值次数</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">成立日</TableHead>
                    <TableHead className="h-8 text-[11px] font-normal text-center">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((p, i) => (
                    <TableRow
                      key={p.id}
                      className="text-[12px] hover:bg-muted/20 cursor-pointer"
                      onClick={() => router.push(`/product-ops/${p.id}`)}
                    >
                      <TableCell className="py-2 text-center text-muted-foreground">{i + 1}</TableCell>
                      <TableCell className="py-2 font-medium">{p.product_name}</TableCell>
                      <TableCell className="py-2 text-center">
                        <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                          p.product_type === "live"
                            ? "bg-green-50 text-green-600 border border-green-200"
                            : "bg-blue-50 text-blue-600 border border-blue-200"
                        }`}>
                          {p.product_type === "live" ? "实盘" : "模拟"}
                        </span>
                      </TableCell>
                      <TableCell className="py-2 text-center text-muted-foreground">
                        {p.custodian || "--"}
                      </TableCell>
                      <TableCell className="py-2 text-center">
                        <NavChange nav={p.latest_nav} />
                      </TableCell>
                      <TableCell className="py-2 text-center tabular-nums text-muted-foreground text-[11px]">
                        {p.latest_valuation_date || "--"}
                      </TableCell>
                      <TableCell className="py-2 text-center tabular-nums">
                        {p.snapshot_count}
                      </TableCell>
                      <TableCell className="py-2 text-center tabular-nums text-muted-foreground text-[11px]">
                        {p.inception_date || "--"}
                      </TableCell>
                      <TableCell className="py-2 text-center">
                        <button
                          className="text-[11px] text-primary hover:underline inline-flex items-center gap-0.5"
                          onClick={(e) => { e.stopPropagation(); router.push(`/product-ops/${p.id}`); }}
                        >
                          详情 <ChevronRight className="h-3 w-3" />
                        </button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </div>
        </TabsContent>
      </Tabs>

      {/* Create Product Dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle className="text-[14px]">新建产品</DialogTitle>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div>
              <label className="text-[11px] text-muted-foreground mb-1 block">产品名称 *</label>
              <Input
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                placeholder="如：博孚利鹭岛晋帆私募证券投资基金"
                className="h-8 text-[12px]"
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[11px] text-muted-foreground mb-1 block">产品代码</label>
                <Input
                  value={formCode}
                  onChange={(e) => setFormCode(e.target.value)}
                  placeholder="如：GF1077"
                  className="h-8 text-[12px]"
                />
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground mb-1 block">产品类型</label>
                <select
                  value={formType}
                  onChange={(e) => setFormType(e.target.value)}
                  className="w-full h-8 rounded border border-input bg-background px-2 text-[12px]"
                >
                  <option value="live">实盘</option>
                  <option value="simulation">模拟</option>
                </select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="text-[11px] text-muted-foreground mb-1 block">托管人</label>
                <Input
                  value={formCustodian}
                  onChange={(e) => setFormCustodian(e.target.value)}
                  placeholder="如：国信证券"
                  className="h-8 text-[12px]"
                />
              </div>
              <div>
                <label className="text-[11px] text-muted-foreground mb-1 block">成立日期</label>
                <Input
                  type="date"
                  value={formInceptionDate}
                  onChange={(e) => setFormInceptionDate(e.target.value)}
                  className="h-8 text-[12px]"
                />
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" className="text-[12px] h-8" onClick={() => setShowCreate(false)}>
              取消
            </Button>
            <Button size="sm" className="text-[12px] h-8" onClick={handleCreate} disabled={creating || !formName.trim()}>
              {creating && <Loader2 className="h-3 w-3 animate-spin mr-1" />}
              创建
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
