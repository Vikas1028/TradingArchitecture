package strategy

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const strategyName = "BTST_LDRB_V1"

type EngineConfig struct {
	Timezone                 string
	SessionStart             string
	EntryStart               string
	EntryEnd                 string
	SameDayExitCutoff        string
	ExitMonitoringEnd        string
	Capital                  float64
	RiskPerTradePct          float64
	MaxTradesPerDay          int
	MaxOpenOvernight         int
	MaxOvernightRiskPct      float64
	CapitalUsageCapPct       float64
	MinPrice                 float64
	MaxIntradayMovePct       float64
	BodyRatioMin             float64
	VolumeRatioMin           float64
	RVOLMin                  float64
	BreakoutCushionPct       float64
	SecondTryExtraPct        float64
	SlippageBufferPct        float64
	MinStopDistancePct       float64
	MaxStopDistancePct       float64
	GapUpPct                 float64
	GapDownPct               float64
	MarketDrawdownLimitPct   float64
	LiquidityTopN            int
	MinPartialFillPct        float64
	RequireSpreadCheck       bool
	MaxSpreadPct             float64
	SwingStart               string
	IndexSymbol              string
	EnableMarketFilter       bool
	EnableEventFilter        bool
	EnableSameDayFailureExit bool
	EnableSafeMode           bool
	AllowRVOLFallback        bool
	SkipAllIfEventsMissing   bool
	EventsPath               string
	VolumeProfilePath        string
	TurnoverRankingPath      string
}

type candle15 struct {
	Start  time.Time
	Close  time.Time
	Open   float64
	High   float64
	Low    float64
	CloseP float64
	Volume int64
}

type symbolDayState struct {
	TradeDate string
	DayOpen   float64
	DayLow    float64
	DayClose  float64
	DayHigh   float64
	CumVol1m  int64
	CumPV1m   float64

	Bars15 []*candle15
	Cur15  *candle15

	IDHPre      float64
	HasIDHPre   bool
	Return1415  float64
	HasReturn14 bool

	TradedToday bool
	Cooldown    bool
}

type positionState struct {
	Symbol         string
	EntryDate      string
	EntryPrice     float64
	EntryQty       int
	RemainingQty   int
	Stop           float64
	RiskAllocated  float64
	BreakoutLevel  float64
	PrevDayLow     float64
	PrevDayClose   float64
	PrevDayHigh    float64
	OpenHandledFor string
	ExitMode       string
}

type indexState struct {
	TradeDate string
	DayOpen   float64
	LastPrice float64
	CumPV     float64
	CumVol    int64
	Window60  []float64
}

type volumeProfile struct {
	CumVolByTime   map[string]float64
	AvgDailyVolume float64
}

type LDRBStrategy struct {
	cfg  EngineConfig
	tz   *time.Location
	log  *zap.Logger
	now  func() time.Time
	safe bool

	sessionStartDur      time.Duration
	entryStartDur        time.Duration
	entryEndDur          time.Duration
	sameDayExitCutoffDur time.Duration
	exitMonitoringEndDur time.Duration
	swingStartDur        time.Duration

	eventsByDate map[string]map[string]bool
	volProfile   map[string]volumeProfile
	liquidTopSet map[string]bool

	index indexState

	symbols   map[string]*symbolDayState
	positions map[string]*positionState
	tradesDay map[string]int
}

func NewLDRBStrategy(cfg EngineConfig, tz *time.Location, logger *zap.Logger) (*LDRBStrategy, error) {
	sessionStartDur, err := parseHHMM(cfg.SessionStart)
	if err != nil {
		return nil, fmt.Errorf("parse session_start: %w", err)
	}
	entryStartDur, err := parseHHMM(cfg.EntryStart)
	if err != nil {
		return nil, fmt.Errorf("parse entry_start: %w", err)
	}
	entryEndDur, err := parseHHMM(cfg.EntryEnd)
	if err != nil {
		return nil, fmt.Errorf("parse entry_end: %w", err)
	}
	sameDayExitCutoffDur, err := parseHHMM(cfg.SameDayExitCutoff)
	if err != nil {
		return nil, fmt.Errorf("parse same_day_exit_cutoff: %w", err)
	}
	exitMonitoringEndDur, err := parseHHMM(cfg.ExitMonitoringEnd)
	if err != nil {
		return nil, fmt.Errorf("parse exit_monitoring_end: %w", err)
	}
	swingStartDur, err := parseHHMM(cfg.SwingStart)
	if err != nil {
		return nil, fmt.Errorf("parse swing_start: %w", err)
	}

	s := &LDRBStrategy{
		cfg:                  cfg,
		tz:                   tz,
		log:                  logger,
		now:                  time.Now,
		sessionStartDur:      sessionStartDur,
		entryStartDur:        entryStartDur,
		entryEndDur:          entryEndDur,
		sameDayExitCutoffDur: sameDayExitCutoffDur,
		exitMonitoringEndDur: exitMonitoringEndDur,
		swingStartDur:        swingStartDur,
		eventsByDate:         make(map[string]map[string]bool),
		volProfile:           make(map[string]volumeProfile),
		liquidTopSet:         make(map[string]bool),
		symbols:              make(map[string]*symbolDayState),
		positions:            make(map[string]*positionState),
		tradesDay:            make(map[string]int),
	}
	s.loadDependencies()
	return s, nil
}

func (s *LDRBStrategy) loadDependencies() {
	missing := make([]string, 0)
	if s.cfg.EnableEventFilter {
		ev, err := loadEvents(s.cfg.EventsPath)
		if err != nil {
			missing = append(missing, "events")
			s.log.Warn("events dependency unavailable", zap.Error(err), zap.String("path", s.cfg.EventsPath))
		} else {
			s.eventsByDate = ev
		}
	}
	vp, err := loadVolumeProfile(s.cfg.VolumeProfilePath)
	if err != nil {
		missing = append(missing, "volume_profile")
		s.log.Warn("volume profile dependency unavailable", zap.Error(err), zap.String("path", s.cfg.VolumeProfilePath))
	} else {
		s.volProfile = vp
	}
	if s.cfg.TurnoverRankingPath != "" {
		top, err := loadTurnoverRanking(s.cfg.TurnoverRankingPath, s.cfg.LiquidityTopN)
		if err != nil {
			s.log.Warn("turnover ranking not loaded; liquidity filter disabled", zap.Error(err))
		} else {
			s.liquidTopSet = top
		}
	}

	if s.cfg.EnableSafeMode {
		if len(s.volProfile) == 0 {
			s.safe = true
		}
		if s.cfg.SkipAllIfEventsMissing && s.cfg.EnableEventFilter && len(s.eventsByDate) == 0 {
			s.safe = true
		}
	}
	if s.safe {
		s.log.Warn("strategy entered safe mode; trades will be skipped", zap.Strings("missing_dependencies", missing))
	}
}

func (s *LDRBStrategy) OnIndexCandle(c Candle) {
	ts := c.Time.In(s.tz)
	tradeDate := ts.Format("2006-01-02")
	if s.cfg.IndexSymbol != "" && !strings.EqualFold(strings.TrimSpace(c.Symbol), strings.TrimSpace(s.cfg.IndexSymbol)) {
		return
	}
	if s.index.TradeDate != tradeDate {
		s.index = indexState{TradeDate: tradeDate}
	}
	if s.index.DayOpen <= 0 {
		s.index.DayOpen = c.Open
	}
	s.index.LastPrice = c.Close
	s.index.CumPV += c.Close * float64(maxInt64(c.Volume, 1))
	s.index.CumVol += maxInt64(c.Volume, 1)
	s.index.Window60 = append(s.index.Window60, c.Close)
	if len(s.index.Window60) > 60 {
		s.index.Window60 = s.index.Window60[len(s.index.Window60)-60:]
	}
}

func (s *LDRBStrategy) OnStockCandle(c Candle) *StrategySignal {
	symbol := strings.ToUpper(strings.TrimSpace(c.Symbol))
	if symbol == "" {
		return nil
	}
	ts := c.Time.In(s.tz)
	state := s.ensureState(symbol, ts)
	state.CumVol1m += c.Volume
	state.CumPV1m += c.Close * float64(maxInt64(c.Volume, 1))
	state.DayClose = c.Close
	if state.DayOpen <= 0 {
		state.DayOpen = c.Open
		state.DayLow = c.Low
		state.DayHigh = c.High
	}
	state.DayLow = math.Min(state.DayLow, c.Low)
	state.DayHigh = math.Max(state.DayHigh, c.High)

	if sig := s.evaluateNextDayExit(symbol, ts, c, state); sig != nil {
		return sig
	}

	closedBar := s.update15mBar(state, ts, c)
	if closedBar == nil {
		return nil
	}
	state.Bars15 = append(state.Bars15, closedBar)
	dayVWAP := 0.0
	if state.CumVol1m > 0 {
		dayVWAP = state.CumPV1m / float64(state.CumVol1m)
	}
	if sig := s.On15mCloseForExit(symbol, closedBar.Close, closedBar.CloseP, dayVWAP); sig != nil {
		return sig
	}
	closeDur := durationOfDay(closedBar.Close)
	if almostEqDur(closeDur, parseDurMust("14:15")) && state.DayOpen > 0 {
		state.Return1415 = (closedBar.CloseP - state.DayOpen) / state.DayOpen
		state.HasReturn14 = true
	}
	if closeDur <= parseDurMust("14:15") {
		if !state.HasIDHPre || closedBar.High > state.IDHPre {
			state.IDHPre = closedBar.High
			state.HasIDHPre = true
		}
	}

	if sig := s.evaluateSameDayFailureExit(symbol, ts, closedBar, state); sig != nil {
		return sig
	}
	return s.evaluateEntry(symbol, ts, closedBar, state)
}

func (s *LDRBStrategy) evaluateEntry(symbol string, ts time.Time, bar *candle15, st *symbolDayState) *StrategySignal {
	if s.safe || st.Cooldown || st.TradedToday {
		return nil
	}
	if _, ok := s.positions[symbol]; ok {
		return nil
	}
	closeDur := durationOfDay(bar.Close)
	if closeDur < s.entryStartDur || closeDur > s.entryEndDur {
		return nil
	}
	if s.cfg.EnableMarketFilter && !s.marketFilterPass() {
		return nil
	}
	if s.tradesDay[st.TradeDate] >= s.cfg.MaxTradesPerDay {
		return nil
	}
	if len(s.positions) >= s.cfg.MaxOpenOvernight {
		return nil
	}
	if len(s.liquidTopSet) > 0 && !s.liquidTopSet[symbol] {
		return nil
	}
	if bar.CloseP < s.cfg.MinPrice {
		return nil
	}
	if !st.HasReturn14 || st.Return1415 > s.cfg.MaxIntradayMovePct {
		return nil
	}
	if s.cfg.EnableEventFilter && s.hasEventNextTradingDay(symbol, bar.Close) {
		return nil
	}
	if !st.HasIDHPre || bar.CloseP <= st.IDHPre {
		return nil
	}

	rangeV := bar.High - bar.Low
	if rangeV <= 0 {
		return nil
	}
	bodyRatio := math.Abs(bar.CloseP-bar.Open) / rangeV
	if bodyRatio < s.cfg.BodyRatioMin {
		return nil
	}

	if len(st.Bars15) < 2 {
		return nil
	}
	prev := st.Bars15[len(st.Bars15)-2]
	if prev.Volume <= 0 {
		return nil
	}
	volRatio := float64(bar.Volume) / float64(prev.Volume)
	if volRatio < s.cfg.VolumeRatioMin {
		return nil
	}

	rvol, ok := s.computeRVOL(symbol, closeDur, st)
	if !ok || rvol < s.cfg.RVOLMin {
		return nil
	}

	sl1 := bar.Low
	sl2, hasSL2 := s.lastSwingLow(st.Bars15, closeDur)
	stop := sl1
	if hasSL2 && sl2 > stop {
		stop = sl2
	}
	entry := bar.CloseP * (1 + s.cfg.BreakoutCushionPct)
	dist := entry - stop
	if dist <= 0 {
		return nil
	}
	if dist < entry*s.cfg.MinStopDistancePct || dist > entry*s.cfg.MaxStopDistancePct {
		return nil
	}

	riskAmt := s.cfg.Capital * s.cfg.RiskPerTradePct
	rps := dist + (entry * s.cfg.SlippageBufferPct)
	if rps <= 0 {
		return nil
	}
	qty := int(math.Floor(riskAmt / rps))
	if qty < 1 {
		return nil
	}
	capQty := int(math.Floor((s.cfg.Capital * s.cfg.CapitalUsageCapPct) / entry))
	if capQty < qty {
		qty = capQty
	}
	if qty < 1 {
		return nil
	}

	overnightRisk := s.totalOpenRisk() + riskAmt
	if overnightRisk > s.cfg.Capital*s.cfg.MaxOvernightRiskPct {
		return nil
	}

	st.TradedToday = true
	s.tradesDay[st.TradeDate]++
	s.positions[symbol] = &positionState{
		Symbol:        symbol,
		EntryDate:     st.TradeDate,
		EntryPrice:    entry,
		EntryQty:      qty,
		RemainingQty:  qty,
		Stop:          stop,
		RiskAllocated: riskAmt,
		BreakoutLevel: st.IDHPre,
		PrevDayLow:    st.DayLow,
		PrevDayHigh:   st.DayHigh,
		PrevDayClose:  st.DayClose,
	}

	return &StrategySignal{
		Strategy: strategyName,
		Symbol:   symbol,
		Side:     SideBuy,
		Time:     bar.Close,
		Reason: fmt.Sprintf(
			"LDRB_ENTRY idh=%.2f body=%.2f vol_ratio=%.2f rvol=%.2f stop=%.2f qty=%d",
			st.IDHPre, bodyRatio, volRatio, rvol, stop, qty,
		),
	}
}

func (s *LDRBStrategy) evaluateSameDayFailureExit(symbol string, ts time.Time, bar *candle15, st *symbolDayState) *StrategySignal {
	if !s.cfg.EnableSameDayFailureExit {
		return nil
	}
	pos, ok := s.positions[symbol]
	if !ok {
		return nil
	}
	if pos.EntryDate != st.TradeDate {
		return nil
	}
	closeDur := durationOfDay(bar.Close)
	if closeDur > parseDurMust("15:15") {
		return nil
	}
	if closeDur > s.sameDayExitCutoffDur {
		return nil
	}
	if bar.CloseP < pos.BreakoutLevel {
		delete(s.positions, symbol)
		return &StrategySignal{
			Strategy: strategyName,
			Symbol:   symbol,
			Side:     SideSell,
			Time:     bar.Close,
			Reason:   "LDRB_EXIT_SAME_DAY_FAILED_BREAKOUT",
		}
	}
	return nil
}

func (s *LDRBStrategy) evaluateNextDayExit(symbol string, ts time.Time, c Candle, st *symbolDayState) *StrategySignal {
	pos, ok := s.positions[symbol]
	if !ok {
		return nil
	}
	tradeDate := ts.Format("2006-01-02")
	if tradeDate == pos.EntryDate {
		pos.PrevDayClose = c.Close
		pos.PrevDayLow = math.Min(pos.PrevDayLow, c.Low)
		pos.PrevDayHigh = math.Max(pos.PrevDayHigh, c.High)
		return nil
	}

	nowDur := durationOfDay(ts)
	if nowDur < s.sessionStartDur || nowDur > s.exitMonitoringEndDur {
		if nowDur > s.exitMonitoringEndDur && pos.RemainingQty > 0 {
			delete(s.positions, symbol)
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     ts,
				Reason:   "LDRB_EXIT_TIMEOUT_1030",
			}
		}
		return nil
	}

	if pos.OpenHandledFor != tradeDate {
		pos.OpenHandledFor = tradeDate
		if pos.PrevDayClose <= 0 {
			delete(s.positions, symbol)
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     ts,
				Reason:   "LDRB_EXIT_DEFENSIVE_NO_PREV_CLOSE",
			}
		}
		gapPct := (c.Open - pos.PrevDayClose) / pos.PrevDayClose
		switch {
		case gapPct >= s.cfg.GapUpPct:
			sellQty := int(math.Max(1, math.Floor(float64(pos.EntryQty)*0.5)))
			pos.RemainingQty -= sellQty
			pos.ExitMode = "GAP_UP_TRAIL"
			if pos.RemainingQty <= 0 {
				delete(s.positions, symbol)
			}
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     ts,
				Reason:   fmt.Sprintf("LDRB_EXIT_GAP_UP_50 gap_pct=%.4f qty=%d", gapPct, sellQty),
			}
		case gapPct < s.cfg.GapDownPct:
			delete(s.positions, symbol)
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     ts,
				Reason:   fmt.Sprintf("LDRB_EXIT_GAP_DOWN gap_pct=%.4f", gapPct),
			}
		default:
			pos.ExitMode = "VWAP_BREAK"
		}
	}

	if pos.RemainingQty <= 0 {
		delete(s.positions, symbol)
		return nil
	}
	return nil
}

func (s *LDRBStrategy) On15mCloseForExit(symbol string, barClose time.Time, barClosePrice float64, dayVWAP float64) *StrategySignal {
	pos, ok := s.positions[symbol]
	if !ok {
		return nil
	}
	if barClose.In(s.tz).Format("2006-01-02") == pos.EntryDate {
		return nil
	}
	nowDur := durationOfDay(barClose.In(s.tz))
	if nowDur > s.exitMonitoringEndDur {
		delete(s.positions, symbol)
		return &StrategySignal{
			Strategy: strategyName,
			Symbol:   symbol,
			Side:     SideSell,
			Time:     barClose,
			Reason:   "LDRB_EXIT_TIMEOUT_1030",
		}
	}
	switch pos.ExitMode {
	case "GAP_UP_TRAIL":
		if barClosePrice < pos.PrevDayLow {
			delete(s.positions, symbol)
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     barClose,
				Reason:   "LDRB_EXIT_TRAIL_PREV_LOW_BREAK",
			}
		}
	case "VWAP_BREAK":
		if dayVWAP > 0 && barClosePrice < dayVWAP {
			delete(s.positions, symbol)
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideSell,
				Time:     barClose,
				Reason:   "LDRB_EXIT_VWAP_BREAK",
			}
		}
	}
	return nil
}

func (s *LDRBStrategy) ensureState(symbol string, ts time.Time) *symbolDayState {
	tradeDate := ts.Format("2006-01-02")
	st, ok := s.symbols[symbol]
	if !ok || st.TradeDate != tradeDate {
		st = &symbolDayState{
			TradeDate: tradeDate,
			DayLow:    math.MaxFloat64,
		}
		s.symbols[symbol] = st
	}
	return st
}

func (s *LDRBStrategy) update15mBar(st *symbolDayState, ts time.Time, c Candle) *candle15 {
	bucketStart, ok := s.bucketStart(ts)
	if !ok {
		return nil
	}
	if st.Cur15 == nil {
		st.Cur15 = &candle15{
			Start:  bucketStart,
			Close:  bucketStart.Add(15 * time.Minute),
			Open:   c.Open,
			High:   c.High,
			Low:    c.Low,
			CloseP: c.Close,
			Volume: c.Volume,
		}
		return nil
	}
	if st.Cur15.Start.Equal(bucketStart) {
		st.Cur15.High = math.Max(st.Cur15.High, c.High)
		st.Cur15.Low = math.Min(st.Cur15.Low, c.Low)
		st.Cur15.CloseP = c.Close
		st.Cur15.Volume += c.Volume
		return nil
	}
	closed := st.Cur15
	st.Cur15 = &candle15{
		Start:  bucketStart,
		Close:  bucketStart.Add(15 * time.Minute),
		Open:   c.Open,
		High:   c.High,
		Low:    c.Low,
		CloseP: c.Close,
		Volume: c.Volume,
	}
	return closed
}

func (s *LDRBStrategy) bucketStart(ts time.Time) (time.Time, bool) {
	t := ts.In(s.tz)
	startDay := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, s.tz).Add(s.sessionStartDur)
	diff := t.Sub(startDay)
	if diff < 0 {
		return time.Time{}, false
	}
	slot := int(diff / (15 * time.Minute))
	return startDay.Add(time.Duration(slot) * 15 * time.Minute), true
}

func (s *LDRBStrategy) computeRVOL(symbol string, tdur time.Duration, st *symbolDayState) (float64, bool) {
	key := formatDurHHMM(tdur)
	p, ok := s.volProfile[symbol]
	if ok {
		if baseline, ok2 := p.CumVolByTime[key]; ok2 && baseline > 0 {
			return float64(s.cumVol15(st)) / baseline, true
		}
		if s.cfg.AllowRVOLFallback && p.AvgDailyVolume > 0 {
			return float64(st.CumVol1m) / p.AvgDailyVolume, true
		}
	}
	return 0, false
}

func (s *LDRBStrategy) cumVol15(st *symbolDayState) int64 {
	var total int64
	for _, b := range st.Bars15 {
		total += b.Volume
	}
	return total
}

func (s *LDRBStrategy) marketFilterPass() bool {
	if !s.cfg.EnableMarketFilter {
		return true
	}
	if s.index.CumVol <= 0 || s.index.LastPrice <= 0 {
		return false
	}
	vwap := s.index.CumPV / float64(s.index.CumVol)
	if s.index.LastPrice >= vwap {
		return true
	}
	if len(s.index.Window60) < 2 || s.index.Window60[0] <= 0 {
		return false
	}
	ret := (s.index.LastPrice - s.index.Window60[0]) / s.index.Window60[0]
	return ret >= s.cfg.MarketDrawdownLimitPct
}

func (s *LDRBStrategy) hasEventNextTradingDay(symbol string, now time.Time) bool {
	next := nextTradingDay(now.In(s.tz)).Format("2006-01-02")
	events, ok := s.eventsByDate[next]
	if !ok {
		return false
	}
	return events[symbol]
}

func (s *LDRBStrategy) totalOpenRisk() float64 {
	total := 0.0
	for _, pos := range s.positions {
		total += pos.RiskAllocated
	}
	return total
}

func (s *LDRBStrategy) lastSwingLow(bars []*candle15, currentCloseDur time.Duration) (float64, bool) {
	if len(bars) < 3 {
		return 0, false
	}
	last := 0.0
	found := false
	maxDur := currentCloseDur - 15*time.Minute
	for i := 1; i < len(bars)-1; i++ {
		closeDur := durationOfDay(bars[i].Close)
		if closeDur < s.swingStartDur || closeDur > maxDur {
			continue
		}
		if bars[i].Low < bars[i-1].Low && bars[i].Low < bars[i+1].Low {
			last = bars[i].Low
			found = true
		}
	}
	return last, found
}

func loadEvents(path string) (map[string]map[string]bool, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("events path empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	out := make(map[string]map[string]bool)
	var byDate map[string][]string
	if err := json.Unmarshal(data, &byDate); err == nil {
		for d, syms := range byDate {
			if out[d] == nil {
				out[d] = make(map[string]bool)
			}
			for _, sym := range syms {
				out[d][strings.ToUpper(strings.TrimSpace(sym))] = true
			}
		}
		return out, nil
	}

	var withEvents struct {
		Events []struct {
			Symbol string `json:"symbol"`
			Date   string `json:"date"`
		} `json:"events"`
	}
	if err := json.Unmarshal(data, &withEvents); err != nil {
		return nil, err
	}
	for _, e := range withEvents.Events {
		d := strings.TrimSpace(e.Date)
		s := strings.ToUpper(strings.TrimSpace(e.Symbol))
		if d == "" || s == "" {
			continue
		}
		if out[d] == nil {
			out[d] = make(map[string]bool)
		}
		out[d][s] = true
	}
	return out, nil
}

func loadVolumeProfile(path string) (map[string]volumeProfile, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("volume profile path empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	type vp struct {
		CumVolByTime   map[string]float64 `json:"cumvol_by_time"`
		AvgDailyVolume float64            `json:"avg_daily_volume_10"`
	}
	out := make(map[string]volumeProfile)

	var direct map[string]vp
	if err := json.Unmarshal(data, &direct); err == nil {
		for sym, v := range direct {
			out[strings.ToUpper(strings.TrimSpace(sym))] = volumeProfile{
				CumVolByTime:   normalizeTimeMap(v.CumVolByTime),
				AvgDailyVolume: v.AvgDailyVolume,
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}

	var wrapped struct {
		Symbols map[string]vp `json:"symbols"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return nil, err
	}
	for sym, v := range wrapped.Symbols {
		out[strings.ToUpper(strings.TrimSpace(sym))] = volumeProfile{
			CumVolByTime:   normalizeTimeMap(v.CumVolByTime),
			AvgDailyVolume: v.AvgDailyVolume,
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no volume profiles parsed")
	}
	return out, nil
}

func loadTurnoverRanking(path string, topN int) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	type pair struct {
		Symbol   string
		Turnover float64
	}
	items := make([]pair, 0)

	var jsonMap map[string]float64
	if err := json.Unmarshal(data, &jsonMap); err == nil {
		for sym, t := range jsonMap {
			items = append(items, pair{Symbol: strings.ToUpper(strings.TrimSpace(sym)), Turnover: t})
		}
	} else {
		r := csv.NewReader(strings.NewReader(string(data)))
		recs, err2 := r.ReadAll()
		if err2 != nil {
			return nil, err2
		}
		for _, rec := range recs {
			if len(rec) < 2 {
				continue
			}
			sym := strings.ToUpper(strings.TrimSpace(rec[0]))
			tv, err3 := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
			if err3 != nil || sym == "" || strings.EqualFold(sym, "symbol") {
				continue
			}
			items = append(items, pair{Symbol: sym, Turnover: tv})
		}
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("no ranking items parsed")
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Turnover > items[j].Turnover })
	limit := minInt(topN, len(items))
	set := make(map[string]bool, limit)
	for i := 0; i < limit; i++ {
		set[items[i].Symbol] = true
	}
	return set, nil
}

func parseHHMM(v string) (time.Duration, error) {
	parts := strings.Split(strings.TrimSpace(v), ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func parseDurMust(v string) time.Duration {
	d, _ := parseHHMM(v)
	return d
}

func durationOfDay(t time.Time) time.Duration {
	tt := t
	return time.Duration(tt.Hour())*time.Hour + time.Duration(tt.Minute())*time.Minute
}

func formatDurHHMM(d time.Duration) string {
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	return fmt.Sprintf("%02d:%02d", h, m)
}

func normalizeTimeMap(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		k = strings.TrimSpace(k)
		if len(k) >= 5 {
			k = k[:5]
		}
		out[k] = v
	}
	return out
}

func nextTradingDay(t time.Time) time.Time {
	n := t.AddDate(0, 0, 1)
	for n.Weekday() == time.Saturday || n.Weekday() == time.Sunday {
		n = n.AddDate(0, 0, 1)
	}
	return n
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func almostEqDur(a, b time.Duration) bool {
	if a > b {
		return a-b < time.Minute
	}
	return b-a < time.Minute
}
