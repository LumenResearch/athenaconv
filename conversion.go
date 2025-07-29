package athenaconv

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/LumenResearch/athenaconv/util"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

func castAthenaRowData(ctx context.Context, rowData types.Datum, athenaType string) (interface{}, error) {
	data := util.SafeString(rowData.VarCharValue)

	var castedData interface{} = nil
	var err error = nil

	// for supported data types, see https://docs.aws.amazon.com/athena/latest/ug/data-types.html
	switch athenaType {
	case "boolean":
		castedData = strings.ToLower(data) == "true"
	case "varchar":
		castedData = data
	case "integer":
		castedData, err = strconv.Atoi(data)
	case "bigint":
		castedData, err = strconv.ParseInt(data, 10, 64)
	case "array":
		arrayValueString := strings.Trim(data, "[]")
		newStringSlice := make([]string, 0)
		if len(arrayValueString) > 0 {
			arrayValue := strings.Split(arrayValueString, ", ")
			newStringSlice = append(newStringSlice, arrayValue...)
		}
		castedData = newStringSlice
	case "map":
		mapData := make(map[string]string)

		fmt.Println(data)

		// Trim surrounding braces
		trimmed := strings.Trim(data, "{}")

		if len(trimmed) > 0 {
			// Split on ", " to get key=value pairs
			pairs := strings.Split(trimmed, ", ")

			for _, pair := range pairs {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					key := kv[0]
					value := kv[1]
					mapData[key] = value
				}
			}
		}

		castedData = mapData
	case "timestamp":
		castedData, err = time.Parse("2006-01-02 15:04:05", data)
	case "date":
		castedData, err = time.Parse("2006-01-02", data)
	default:
		log.Printf("ATHENA DATA TYPE NOT SUPPORTED: '%s', defaulting to string %s\n", athenaType, data)
		castedData = data
	}

	return castedData, err
}
