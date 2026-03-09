"use client";

import { useState, useCallback, useRef } from "react";
import { useRouter } from "next/navigation";
import {
  Upload, FileSpreadsheet, Loader2, ChevronRight, Eye,
  Download, Trash2, AlertTriangle, CheckCircle, Clock,
  FileText, Search, RefreshCw,
} from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { fetchApi } from "@/lib/api";

/* --- Types --- */

interface ParsedHolding {
  level: number;
  item_code: string;
  item_name: string;
  quantity: number | null;
  unit_cost: number | null;
  cost_amount: number | null;
  cost_pct_nav: number | null;
  market_price: number | null;
  market_value: number | null;
  value_pct_nav: number | null;
  value_diff: number | null;
}

interface ParsedValuation {
  product_name: string;
  valuation_date: string;
  unit_nav: number;
  total_nav: number;
  total_shares: number;
  holdings: ParsedHolding[];
  sub_funds: {
    filing_number: string;
    fund_name: string;
    market_value: number;
    weight_pct: number;
    appreciation: number | null;
  }[];
}

interface UploadResult {
  success: boolean;
  snapshot_id?: number;
  product_id?: number;
  message: string;
  warnings: string[];
}

/* --- Helpers --- */

function formatMoney(value: number | null): string {
  if (value === null || value === undefined) return "--";
  if (Math.abs(value) >= 1e8) return `${(value / 1e8).toFixed(2)}亿`;
  if (Math.abs(value) >= 1e4) return `${(value / 1e4).toFixed(2)}万`;
  return value.toFixed(2);
}

function formatPct(value: number | null): string {
  if (value === null || value === undefined) return "--";
  return `${(value * 100).toFixed(2)}%`;
}

/* --- Components --- */

function LevelBadge({ level }: { level: number }) {
  const colors = [
    "bg-slate-100 text-slate-700 border-slate-200",
    "bg-blue-50 text-blue-700 border-blue-200",
    "bg-green-50 text-green-700 border-green-200",
    "bg-amber-50 text-amber-700 border-amber-200",
    "bg-purple-50 text-purple-700 border-purple-200",
  ];
  const labels = ["总", "一级", "二级", "三级", "四级"];
  return (
    <Badge variant="outline" className={`text-[10px] ${colors[level] || colors[0]}`}>
      {labels[level] || `L${level}`}
    </Badge>
  );
}

/* --- Page --- */

export default function ValuationPage() {
  const router = useRouter();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [activeTab, setActiveTab] = useState("upload");
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
  const [parsedData, setParsedData] = useState<ParsedValuation | null>(null);
  const [showParsedDialog, setShowParsedDialog] = useState(false);

  // File upload handler
  const handleFileUpload = useCallback(async (file: File) => {
    setUploading(true);
    setUploadResult(null);
    try {
      const formData = new FormData();
      formData.append("file", file);

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1"}/valuation/parse`,
        { method: "POST", body: formData }
      );

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `解析失败: ${res.status}`);
      }

      const result: ParsedValuation = await res.json();
      setParsedData(result);
      setShowParsedDialog(true);
      setUploadResult({
        success: true,
        message: `成功解析 ${result.product_name} 的估值表`,
        warnings: [],
      });
    } catch (e: any) {
      setUploadResult({
        success: false,
        message: e.message || "解析失败",
        warnings: [],
      });
    } finally {
      setUploading(false);
    }
  }, []);

  return (
    <div className="space-y-4">
      <PageHeader
        title="四级估值表解析"
        description="解析博富利鹭岛金帆FOF等产品的四级估值表"
      />

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="h-8">
          <TabsTrigger value="upload" className="text-[12px] h-7 px-3">
            <Upload className="h-3.5 w-3.5 mr-1.5" />
            上传解析
          </TabsTrigger>
          <TabsTrigger value="history" className="text-[12px] h-7 px-3">
            <Clock className="h-3.5 w-3.5 mr-1.5" />
            历史记录
          </TabsTrigger>
          <TabsTrigger value="email" className="text-[12px] h-7 px-3">
            <FileText className="h-3.5 w-3.5 mr-1.5" />
            邮箱抓取
          </TabsTrigger>
        </TabsList>

        {/* Upload Tab */}
        <TabsContent value="upload" className="mt-4 space-y-4">
          {/* Upload Area */}
          <div
            className="border-2 border-dashed border-border rounded-lg p-8 text-center hover:bg-muted/50 transition-colors cursor-pointer"
            onClick={() => fileInputRef.current?.click()}
            onDrop={(e) => {
              e.preventDefault();
              const file = e.dataTransfer.files[0];
              if (file) handleFileUpload(file);
            }}
            onDragOver={(e) => e.preventDefault()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls,.pdf"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleFileUpload(file);
                e.target.value = "";
              }}
            />
            {uploading ? (
              <div className="flex flex-col items-center gap-2">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
                <p className="text-[13px] text-muted-foreground">正在解析估值表...</p>
              </div>
            ) : (
              <div className="flex flex-col items-center gap-2">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center">
                  <FileSpreadsheet className="h-6 w-6 text-primary" />
                </div>
                <p className="text-[13px] font-medium">点击或拖拽上传估值表</p>
                <p className="text-[11px] text-muted-foreground">
                  支持 Excel (.xlsx, .xls) 和 PDF 格式
                </p>
              </div>
            )}
          </div>

          {/* Upload Result */}
          {uploadResult && (
            <div className={`p-4 rounded border ${uploadResult.success ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
              <div className="flex items-start gap-2">
                {uploadResult.success ? (
                  <CheckCircle className="h-4 w-4 text-green-600 shrink-0 mt-0.5" />
                ) : (
                  <AlertTriangle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                )}
                <div>
                  <p className={`text-[13px] font-medium ${uploadResult.success ? 'text-green-800' : 'text-red-800'}`}>
                    {uploadResult.message}
                  </p>
                  {uploadResult.warnings.length > 0 && (
                    <ul className="mt-2 space-y-1">
                      {uploadResult.warnings.map((w, i) => (
                        <li key={i} className="text-[11px] text-amber-700 flex items-center gap-1">
                          <AlertTriangle className="h-3 w-3" />
                          {w}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Instructions */}
          <div className="bg-muted/30 rounded-lg p-4">
            <h4 className="text-[13px] font-medium mb-3">支持解析的估值表格式</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <p className="text-[12px] font-medium text-muted-foreground">博富利鹭岛金帆FOF</p>
                <ul className="text-[11px] text-muted-foreground space-y-1">
                  <li>• 四级科目结构：科目代码 / 科目名称</li>
                  <li>• 数量、单位成本、成本金额</li>
                  <li>• 市价、市值、市值占比</li>
                  <li>• 估值增值</li>
                </ul>
              </div>
              <div className="space-y-2">
                <p className="text-[12px] font-medium text-muted-foreground">通用估值表</p>
                <ul className="text-[11px] text-muted-foreground space-y-1">
                  <li>• 支持多级科目层级解析</li>
                  <li>• 自动识别子基金持仓</li>
                  <li>• 自动计算权重占比</li>
                  <li>• 支持自定义字段映射</li>
                </ul>
              </div>
            </div>
          </div>
        </TabsContent>

        {/* History Tab */}
        <TabsContent value="history" className="mt-4">
          <div className="bg-card border border-border rounded overflow-hidden">
            <div className="p-4 border-b border-border">
              <div className="flex items-center justify-between">
                <h3 className="text-[13px] font-medium">解析历史</h3>
                <div className="flex items-center gap-2">
                  <Input
                    placeholder="搜索产品名称..."
                    className="h-7 text-[12px] w-48"
                  />
                  <Button variant="outline" size="sm" className="h-7 text-[11px]">
                    <Search className="h-3.5 w-3.5 mr-1" />
                    搜索
                  </Button>
                </div>
              </div>
            </div>
            <div className="p-8 text-center text-muted-foreground">
              <Clock className="h-10 w-10 mx-auto mb-3 opacity-20" />
              <p className="text-[13px]">暂无解析历史</p>
              <p className="text-[11px] opacity-60 mt-1">上传估值表后将显示解析记录</p>
            </div>
          </div>
        </TabsContent>

        {/* Email Tab */}
        <TabsContent value="email" className="mt-4">
          <div className="bg-card border border-border rounded p-6">
            <div className="text-center space-y-3">
              <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mx-auto">
                <RefreshCw className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-[14px] font-medium">邮箱自动抓取</h3>
              <p className="text-[12px] text-muted-foreground max-w-md mx-auto">
                系统将自动从配置的邮箱中抓取估值表邮件，解析并入库。
                支持定时任务和手动触发。
              </p>
              <div className="flex items-center justify-center gap-2 pt-2">
                <Button size="sm" className="text-[12px]">
                  <RefreshCw className="h-3.5 w-3.5 mr-1.5" />
                  手动抓取
                </Button>
                <Button variant="outline" size="sm" className="text-[12px]">
                  配置邮箱
                </Button>
              </div>
            </div>
          </div>
        </TabsContent>
      </Tabs>

      {/* Parsed Data Dialog */}
      <Dialog open={showParsedDialog} onOpenChange={setShowParsedDialog}>
        <DialogContent className="max-w-5xl max-h-[80vh] overflow-hidden flex flex-col">
          <DialogHeader>
            <DialogTitle className="text-[14px]">
              估值表解析结果 - {parsedData?.product_name}
            </DialogTitle>
          </DialogHeader>
          {parsedData && (
            <div className="flex-1 overflow-auto space-y-4 mt-2">
              {/* Summary Cards */}
              <div className="grid grid-cols-4 gap-3">
                <div className="bg-muted/40 rounded p-3">
                  <p className="text-[11px] text-muted-foreground">估值日期</p>
                  <p className="text-[14px] font-semibold">{parsedData.valuation_date}</p>
                </div>
                <div className="bg-muted/40 rounded p-3">
                  <p className="text-[11px] text-muted-foreground">单位净值</p>
                  <p className="text-[14px] font-semibold tabular-nums">{parsedData.unit_nav.toFixed(4)}</p>
                </div>
                <div className="bg-muted/40 rounded p-3">
                  <p className="text-[11px] text-muted-foreground">资产净值</p>
                  <p className="text-[14px] font-semibold tabular-nums">{formatMoney(parsedData.total_nav)}</p>
                </div>
                <div className="bg-muted/40 rounded p-3">
                  <p className="text-[11px] text-muted-foreground">总份额</p>
                  <p className="text-[14px] font-semibold tabular-nums">{formatMoney(parsedData.total_shares)}</p>
                </div>
              </div>

              {/* Holdings Table */}
              <div>
                <h4 className="text-[13px] font-medium mb-2">四级科目明细</h4>
                <div className="border rounded overflow-hidden">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-muted/40">
                        <TableHead className="h-8 text-[11px] font-normal">层级</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal">科目代码</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal">科目名称</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">数量</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">单位成本</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">成本金额</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                        <TableHead className="h-8 text-[11px] font-normal text-right">占比</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {parsedData.holdings.map((h, i) => (
                        <TableRow key={i} className="text-[12px]">
                          <TableCell className="py-1.5">
                            <LevelBadge level={h.level} />
                          </TableCell>
                          <TableCell className="py-1.5 font-mono text-[11px]">{h.item_code}</TableCell>
                          <TableCell className="py-1.5">
                            <span style={{ paddingLeft: `${(h.level - 1) * 12}px` }}>
                              {h.item_name}
                            </span>
                          </TableCell>
                          <TableCell className="py-1.5 text-right tabular-nums">
                            {h.quantity?.toFixed(2) ?? '--'}
                          </TableCell>
                          <TableCell className="py-1.5 text-right tabular-nums">
                            {h.unit_cost?.toFixed(4) ?? '--'}
                          </TableCell>
                          <TableCell className="py-1.5 text-right tabular-nums">
                            {formatMoney(h.cost_amount)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right tabular-nums">
                            {formatMoney(h.market_value)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right tabular-nums">
                            {formatPct(h.value_pct_nav)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>

              {/* Sub Funds */}
              {parsedData.sub_funds.length > 0 && (
                <div>
                  <h4 className="text-[13px] font-medium mb-2">子基金持仓</h4>
                  <div className="border rounded overflow-hidden">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-muted/40">
                          <TableHead className="h-8 text-[11px] font-normal">备案号</TableHead>
                          <TableHead className="h-8 text-[11px] font-normal">基金名称</TableHead>
                          <TableHead className="h-8 text-[11px] font-normal text-right">市值</TableHead>
                          <TableHead className="h-8 text-[11px] font-normal text-right">权重</TableHead>
                          <TableHead className="h-8 text-[11px] font-normal text-right">浮盈</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {parsedData.sub_funds.map((sf, i) => (
                          <TableRow key={i} className="text-[12px]">
                            <TableCell className="py-1.5 font-mono text-[11px]">{sf.filing_number}</TableCell>
                            <TableCell className="py-1.5 font-medium">{sf.fund_name}</TableCell>
                            <TableCell className="py-1.5 text-right tabular-nums">{formatMoney(sf.market_value)}</TableCell>
                            <TableCell className="py-1.5 text-right tabular-nums">{formatPct(sf.weight_pct)}</TableCell>
                            <TableCell className={`py-1.5 text-right tabular-nums ${(sf.appreciation ?? 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {formatMoney(sf.appreciation)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}
            </div>
          )}
          <div className="flex items-center justify-end gap-2 mt-4 pt-4 border-t">
            <Button variant="outline" size="sm" className="text-[12px]">
              <Download className="h-3.5 w-3.5 mr-1.5" />
              导出Excel
            </Button>
            <Button size="sm" className="text-[12px]">
              保存到产品
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
