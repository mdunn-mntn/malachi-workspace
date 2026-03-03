import React, { useState, useMemo } from 'react';

export default function ReachPerformanceViz() {
  const [spend, setSpend] = useState(50000);
  const [targetingLevel, setTargetingLevel] = useState(50);

  const CPM = 25;
  const TOTAL_HOUSEHOLDS = 50000000;
  const BASE_IVR = 0.003;
  const MAX_IVR = 0.018;

  const metrics = useMemo(() => {
    const t = targetingLevel / 100;
    const impressions = (spend / CPM) * 1000;
    const reachMultiplier = Math.pow(1 - t, 1.5);
    const potentialHouseholds = Math.round(TOTAL_HOUSEHOLDS * Math.max(0.02, reachMultiplier));
    const ivrMultiplier = 1 + (Math.pow(t, 0.7) * ((MAX_IVR / BASE_IVR) - 1));
    const ivr = BASE_IVR * ivrMultiplier;
    const estimatedVisits = Math.round(impressions * ivr);
    
    return { impressions, potentialHouseholds, ivr, estimatedVisits };
  }, [spend, targetingLevel]);

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(0) + 'K';
    return num.toLocaleString();
  };

  const ivrPosition = ((metrics.ivr - BASE_IVR) / (MAX_IVR - BASE_IVR)) * 100;
  const reachPosition = (metrics.potentialHouseholds / TOTAL_HOUSEHOLDS) * 100;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 to-slate-800 p-8 flex items-center justify-center">
      <div className="w-full max-w-2xl">
        <h1 className="text-3xl font-light text-white mb-8 text-center tracking-tight">
          Reach vs. Performance Tradeoff
        </h1>

        {/* Spend Input */}
        <div className="bg-slate-800/50 backdrop-blur rounded-2xl p-6 mb-5 border border-slate-700/50">
          <label className="text-slate-400 text-xs uppercase tracking-widest mb-3 block">
            Monthly Spend
          </label>
          <div className="flex items-baseline gap-2">
            <span className="text-slate-500 text-3xl font-light">$</span>
            <input
              type="number"
              value={spend}
              onChange={(e) => setSpend(Math.max(0, Number(e.target.value)))}
              className="bg-transparent text-5xl font-light text-white outline-none w-full tracking-tight"
            />
          </div>
        </div>

        {/* Targeting Slider */}
        <div className="bg-slate-800/50 backdrop-blur rounded-2xl p-6 mb-5 border border-slate-700/50">
          <label className="text-slate-400 text-xs uppercase tracking-widest mb-4 block">
            Audience Targeting
          </label>
          <div className="flex justify-between text-xs text-slate-500 mb-3 px-1">
            <span>← Broad Reach</span>
            <span>High Performance →</span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            value={targetingLevel}
            onChange={(e) => setTargetingLevel(Number(e.target.value))}
            className="w-full h-2 bg-slate-700 rounded-full appearance-none cursor-pointer"
            style={{
              background: `linear-gradient(to right, #0891b2 0%, #0891b2 ${targetingLevel}%, #334155 ${targetingLevel}%, #334155 100%)`
            }}
          />
        </div>

        {/* Metrics Cards */}
        <div className="grid grid-cols-2 gap-4 mb-5">
          <div className="bg-slate-800/50 backdrop-blur rounded-2xl p-5 border border-slate-700/50">
            <div className="text-slate-400 text-xs uppercase tracking-widest mb-1">
              Potential Households
            </div>
            <div className="text-3xl font-light text-white tracking-tight">
              {formatNumber(metrics.potentialHouseholds)}
            </div>
            {/* Mini reach bar */}
            <div className="mt-3 h-1.5 bg-slate-700 rounded-full overflow-hidden">
              <div 
                className="h-full bg-gradient-to-r from-cyan-500 to-cyan-400 rounded-full transition-all duration-300"
                style={{ width: `${reachPosition}%` }}
              />
            </div>
          </div>

          <div className="bg-slate-800/50 backdrop-blur rounded-2xl p-5 border border-slate-700/50">
            <div className="text-slate-400 text-xs uppercase tracking-widest mb-1">
              Est. Site Visits
            </div>
            <div className="text-3xl font-light text-white tracking-tight">
              {formatNumber(metrics.estimatedVisits)}
            </div>
            <div className="mt-3 text-slate-500 text-xs">
              from {formatNumber(metrics.impressions)} impressions
            </div>
          </div>
        </div>

        {/* IVR Performance Scale */}
        <div className="bg-slate-800/50 backdrop-blur rounded-2xl p-6 border border-slate-700/50">
          <div className="flex justify-between items-baseline mb-4">
            <span className="text-slate-400 text-xs uppercase tracking-widest">
              IVR (Impressions to Visits)
            </span>
            <span className="text-2xl font-light text-cyan-400">
              {(metrics.ivr * 100).toFixed(2)}%
            </span>
          </div>
          
          {/* Scale visualization */}
          <div className="relative h-12 bg-slate-700/50 rounded-xl overflow-hidden">
            {/* Gradient background */}
            <div className="absolute inset-0 bg-gradient-to-r from-slate-600 via-cyan-900 to-cyan-600 opacity-50" />
            
            {/* Scale markers */}
            <div className="absolute inset-x-4 top-1 flex justify-between text-[10px] text-slate-400">
              <span>{(BASE_IVR * 100).toFixed(1)}%</span>
              <span>{(MAX_IVR * 100).toFixed(1)}%</span>
            </div>
            
            {/* Moving dot */}
            <div 
              className="absolute top-1/2 -translate-y-1/2 transition-all duration-300 ease-out"
              style={{ left: `calc(${ivrPosition}% - 12px + 16px * (1 - ${ivrPosition}/100))` }}
            >
              <div className="w-6 h-6 bg-cyan-400 rounded-full shadow-lg shadow-cyan-400/50 flex items-center justify-center">
                <div className="w-2 h-2 bg-white rounded-full" />
              </div>
            </div>
          </div>
          
          <div className="flex justify-between text-xs text-slate-500 mt-2 px-1">
            <span>Lower Performance</span>
            <span>Higher Performance</span>
          </div>
        </div>

        {/* Footer note */}
        <p className="text-center text-slate-600 text-xs mt-6">
          Tighter targeting reduces reach but improves visit rate
        </p>
      </div>
    </div>
  );
}
