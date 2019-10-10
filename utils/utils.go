package db_utils

import (
	"database/sql"
	"strconv"
	"strings"
	"time"
)

func StringToFloatSlice(s string, a []float64) []float64 {
	r := strings.Trim(s, "{}")
	if a == nil {
		a = make([]float64, 0, 10)
	}
	for _, t := range strings.Split(r, ",") {
		i, _ := strconv.ParseFloat(t, 64)
		a = append(a, i)
	}
	return a
}

func StringToIntSlice(s string) []int {
	r := strings.Trim(s, "{}")
	a := make([]int, 0, 10)
	for _, t := range strings.Split(r, ",") {
		i, _ := strconv.Atoi(t)
		a = append(a, i)
	}
	return a
}

func StringToStringSlice(s string) []string {
	r := strings.Trim(s, "{\"\"}")
	a := make([]string, 0, 10)
	for _, s := range strings.Split(r, ",") {
		a = append(a, s)
	}
	return a
}

func StringSliceToString(sl []string) string {
	r := "{"
	total := len(sl)
	for i := range sl {
		if sl[i] == "" {
			continue
		}
		r = r + sl[i]
		if i < (total - 1) {
			r = r + ","
		}
	}
	return r + "}"
}

func ResultInterfaceToTime(result interface{}) time.Time {
	if result != nil {
		return result.(time.Time)
	}
	return time.Time{}
}

func NullStringToTime(str *sql.NullString) time.Time {
	if str.Valid {
		if str.String != "0000-00-00 00:00:00" {
			t, _ := time.Parse("2006-01-02 15:04:05", str.String)
			return t
		}
	}
	return time.Time{}
}

func NullStringToTimePtr(str *sql.NullString) *time.Time {
	t := NullStringToTime(str)
	if t.Year() == 1 {
		return nil
	}
	return &t
}

func NullStringToString(str *sql.NullString) string {
	if str.Valid {
		return str.String
	}
	return ""

}

func NullFloatToFloat(flt *sql.NullFloat64) float64 {
	if flt.Valid {
		return flt.Float64
	}
	return 0

}

func NullInt64ToInt64(i *sql.NullInt64) int64 {
	if i.Valid {
		return i.Int64
	}
	return 0
}

func NullInt64ToInt32(i *sql.NullInt64) int32 {
	if i.Valid {
		return int32(i.Int64)
	}
	return 0
}

func NullInt64ToInt(i *sql.NullInt64) int {
	if i.Valid {
		return int(i.Int64)
	}
	return 0
}

func NullBoolToBool(b *sql.NullBool) bool {
	if b.Valid {
		return b.Bool
	}
	return false
}

func NullStringIfEmpty(s string) sql.NullString {
    if len(s) == 0 {
        return sql.NullString{}
    }
    return sql.NullString{
         String: s,
         Valid: true,
    }
}

// func NullInt32IfZero(i int) sql.NullInt32 {
// 	if i == 0 {
// 		return sql.NullInt32{}
// 	}
// 	return sql.NullInt32{
// 		Int32: int32(i),
// 		Valid: true,
// 	}
// }

func NullInt64IfZero(i int) sql.NullInt64 {
	if i == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{
		Int64: int64(i),
		Valid: true,
	}
}