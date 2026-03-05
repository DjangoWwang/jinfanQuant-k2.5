"use client";

interface IntervalOption {
  key: string;
  label: string;
}

const defaultIntervals: IntervalOption[] = [
  { key: "1w", label: "近1周" },
  { key: "1m", label: "近1月" },
  { key: "3m", label: "近3月" },
  { key: "6m", label: "近6月" },
  { key: "ytd", label: "今年以来" },
  { key: "1y", label: "近1年" },
  { key: "2y", label: "近2年" },
  { key: "3y", label: "近3年" },
  { key: "inception", label: "成立以来" },
];

interface IntervalSelectorProps {
  value: string;
  onChange: (key: string) => void;
  intervals?: IntervalOption[];
}

export function IntervalSelector({
  value,
  onChange,
  intervals = defaultIntervals,
}: IntervalSelectorProps) {
  return (
    <div className="flex gap-1 flex-wrap">
      {intervals.map((iv) => (
        <button
          key={iv.key}
          onClick={() => onChange(iv.key)}
          className={`px-2.5 py-1 rounded text-[11px] transition-colors ${
            value === iv.key
              ? "bg-primary text-primary-foreground shadow-sm"
              : "bg-muted text-muted-foreground hover:bg-muted/80"
          }`}
        >
          {iv.label}
        </button>
      ))}
    </div>
  );
}
