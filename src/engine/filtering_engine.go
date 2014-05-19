package engine

import (
	"parser"
	p "protocol"

	log "code.google.com/p/log4go"
)

type FilteringEngine struct {
	query        *parser.SelectQuery
	processor    QueryProcessor
	shouldFilter bool
}

func NewFilteringEngine(query *parser.SelectQuery, processor QueryProcessor) *FilteringEngine {
	shouldFilter := query.GetWhereCondition() != nil
	return &FilteringEngine{query, processor, shouldFilter}
}

// optimize for yield series and use it here
func (self *FilteringEngine) YieldPoint(seriesName *string, columnNames []string, point *p.Point) bool {
	return self.YieldSeries(&p.Series{
		Name:   seriesName,
		Fields: columnNames,
		Points: []*p.Point{point},
	})
}

func (self *FilteringEngine) YieldSeries(seriesIncoming *p.Series) bool {
	series := seriesIncoming

	fromClause := self.query.GetFromClause()
	if fromClause.FunctionCall != nil {
		s, err := ApplyFunction(fromClause.FunctionCall, seriesIncoming)
		if err != nil {
			log.Error("Error while applying function to series: %s [query: %s]", err, self.query.GetQueryString())
			return false
		}
		series = s
	}

	if !self.shouldFilter {
		return self.processor.YieldSeries(series)
	}

	series, err := Filter(self.query, series)
	if err != nil {
		log.Error("Error while filtering points: %s [query = %s]", err, self.query.GetQueryString())
		return false
	}
	if len(series.Points) == 0 {
		return true
	}
	return self.processor.YieldSeries(series)
}

func (self *FilteringEngine) Close() {
	self.processor.Close()
}

func (self *FilteringEngine) SetShardInfo(shardId int, shardLocal bool) {
	self.processor.SetShardInfo(shardId, shardLocal)
}
func (self *FilteringEngine) GetName() string {
	return self.processor.GetName()
}
