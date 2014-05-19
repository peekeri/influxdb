package engine

import (
	"fmt"
	"parser"
	"protocol"
	"strconv"
	"strings"
)

type SeriesMappingOperator func(functionCall *parser.Value, seriesIncoming *protocol.Series) (*protocol.Series, error)

var registeredFunctions = make(map[string]SeriesMappingOperator)

func init() {
	registeredFunctions = make(map[string]SeriesMappingOperator)
	registeredFunctions["scale"] = ScaleFunction
	registeredFunctions["timeshift"] = TimeShiftFunction
}

func ApplyFunction(functionCall *parser.Value, seriesIncoming *protocol.Series) (*protocol.Series, error) {
	functionName := strings.ToLower(functionCall.Name)
	fun := registeredFunctions[functionName]
	if fun == nil {
		return nil, fmt.Errorf("Unknown function %s", functionName)
	}

	return fun(functionCall, seriesIncoming)
}

func position(name string, values []string) int {
	for i, value := range values {
		if name == value {
			return i
		}
	}
	return -1
}

func ScaleFunction(functionCall *parser.Value, series *protocol.Series) (*protocol.Series, error) {
	if len(functionCall.Elems) != 2 {
		return nil, fmt.Errorf("%s takes exactly 2 parameters", functionCall.Name)
	}

	if functionCall.Elems[1].Type != parser.ValueInt && functionCall.Elems[1].Type != parser.ValueFloat {
		return nil, fmt.Errorf("Second parameter of %s must be int or float", functionCall.Name)
	}

	scaleFactor, err := strconv.ParseFloat(functionCall.Elems[1].Name, 64)
	if err != nil {
		return nil, err
	}

	valuePosition := position("value", series.Fields)
	if valuePosition == -1 {
		return series, nil
	}

	points := series.Points
	series.Points = nil

	for _, point := range points {
		if point.Values[valuePosition].DoubleValue != nil {
			*point.Values[valuePosition].DoubleValue = *point.Values[valuePosition].DoubleValue * scaleFactor
		} else if point.Values[valuePosition].Int64Value != nil {
			newValue := float64(*point.Values[valuePosition].Int64Value) * scaleFactor
			point.Values[valuePosition].DoubleValue = &newValue
			point.Values[valuePosition].Int64Value = nil
		}
		series.Points = append(series.Points, point)
	}

	return series, nil
}

func TimeShiftFunction(functionCall *parser.Value, series *protocol.Series) (*protocol.Series, error) {
	if len(functionCall.Elems) != 2 {
		return nil, fmt.Errorf("%s takes exactly 2 parameters", functionCall.Name)
	}

	if functionCall.Elems[1].Type != parser.ValueInt {
		return nil, fmt.Errorf("Second parameter of %s must be int", functionCall.Name)
	}

	timeShift, err := strconv.ParseInt(functionCall.Elems[1].Name, 10, 64)
	if err != nil {
		return nil, err
	}

	points := series.Points
	series.Points = nil

	for _, point := range points {
		newValue := int64(*point.Timestamp) + timeShift
		point.Timestamp = &newValue
		series.Points = append(series.Points, point)
	}

	return series, nil
}
