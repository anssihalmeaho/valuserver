package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/anssihalmeaho/funl/funl"
	"github.com/anssihalmeaho/funl/std"

	"github.com/anssihalmeaho/fuvaluez/fuvaluez"
)

func getRPCPutValue(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	rpcPutValue := func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		if len(arguments) < 2 {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: "needs 2 arguments",
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}
		if arguments[0].Kind != funl.StringValue {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: "Assuming string as col name",
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}
		colName := arguments[0].Data.(string)
		colinfo, err := db.GetCollection(colName)
		if err != nil {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: fmt.Sprintf("Error in collection: %v", err),
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}

		putArgs := []funl.Value{
			{
				Kind: funl.OpaqueValue,
				Data: colinfo.Col,
			},
			arguments[1],
		}
		retVal = vzPutValue(db.TopFrame, putArgs)
		return
	}
	return rpcPutValue
}

func getRPChandler(db *Database, vzHandler fuvaluez.FZProc) func(*funl.Frame, []funl.Value) funl.Value {
	rpcHandler := func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		if len(arguments) < 2 {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: "needs 2 arguments",
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}
		if arguments[0].Kind != funl.StringValue {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: "Assuming string as col name",
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}
		colName := arguments[0].Data.(string)
		colinfo, err := db.GetCollection(colName)
		if err != nil {
			rvals := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: fmt.Sprintf("Error in collection: %v", err),
				},
			}
			retVal = funl.MakeListOfValues(frame, rvals)
			return
		}

		conArgs := []*funl.Item{
			&funl.Item{
				Type: funl.ValueItem,
				Data: db.Converter,
			},
			&funl.Item{
				Type: funl.ValueItem,
				Data: arguments[1],
			},
		}
		funcVal := funl.HandleCallOP(db.TopFrame, conArgs)
		handlerArgs := []funl.Value{
			{
				Kind: funl.OpaqueValue,
				Data: colinfo.Col,
			},
			funcVal,
		}
		retVal = vzHandler(db.TopFrame, handlerArgs)
		return
	}
	return rpcHandler
}

func getRPCTakeValues(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	return getRPChandler(db, vzTakeValues)
}

func getRPCUpdate(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	return getRPChandler(db, vzUpdate)
}

func getRPCGetValues(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	return getRPChandler(db, vzGetValues)
}

var vzOpen fuvaluez.FZProc
var vzGetColNames fuvaluez.FZProc
var vzGetCol fuvaluez.FZProc
var vzNewCol fuvaluez.FZProc

var vzClose fuvaluez.FZProc
var vzPutValue fuvaluez.FZProc
var vzGetValues fuvaluez.FZProc
var vzTakeValues fuvaluez.FZProc
var vzUpdate fuvaluez.FZProc

// ColInfo represents valuez col
type ColInfo struct {
	Name string
	Col  *fuvaluez.OpaqueCol
}

// Database represents valuez db
type Database struct {
	colsLock  sync.RWMutex
	TopFrame  *funl.Frame
	Cols      map[string]*ColInfo
	Db        *fuvaluez.OpaqueDB
	Converter funl.Value
}

// Shutdown closes db
func (db *Database) Shutdown() {
	closeArgs := []funl.Value{
		{
			Kind: funl.OpaqueValue,
			Data: db.Db,
		},
	}
	vzClose(db.TopFrame, closeArgs)
}

func convertToSlice(listv funl.Value) ([]interface{}, error) {
	if listv.Kind != funl.ListValue {
		return nil, fmt.Errorf("Not list value: %v", listv)
	}
	lit := funl.NewListIterator(listv)
	resultSlice := []interface{}{}
	for {
		lval := lit.Next()
		if lval == nil {
			break
		}
		resultSlice = append(resultSlice, (*lval).Data)
	}
	return resultSlice, nil
}

// GetCollection gets collection
func (db *Database) GetCollection(colName string) (*ColInfo, error) {
	db.colsLock.RLock()
	col, found := db.Cols[colName]
	db.colsLock.RUnlock()
	if found {
		return col, nil
	}

	colArgs := []funl.Value{
		{
			Kind: funl.OpaqueValue,
			Data: db.Db,
		},
		{
			Kind: funl.StringValue,
			Data: colName,
		},
	}
	colRetList, err := convertToSlice(vzNewCol(db.TopFrame, colArgs))
	if err != nil {
		return nil, err
	}
	if colRetList[0].(bool) != true {
		return nil, fmt.Errorf("creating col failed: %s", colRetList[1].(string))
	}

	colInfo := &ColInfo{
		Name: colName,
		Col:  colRetList[2].(*fuvaluez.OpaqueCol),
	}

	db.colsLock.Lock()
	db.Cols[colName] = colInfo
	db.colsLock.Unlock()

	return colInfo, nil
}

// OpenDatabase opens database
func OpenDatabase(frame *funl.Frame, config *conf) (*Database, error) {
	openArgs := []funl.Value{
		{
			Kind: funl.StringValue,
			Data: config.getVal("filepath") + config.getVal("dbname"),
		},
	}
	openRetList, err := convertToSlice(vzOpen(frame, openArgs))
	if err != nil {
		return nil, err
	}
	if openRetList[0].(bool) != true {
		return nil, fmt.Errorf("open failed: %s", openRetList[1].(string))
	}
	opaqueDB := openRetList[2].(*fuvaluez.OpaqueDB)

	colnamesArgs := []funl.Value{
		{
			Kind: funl.OpaqueValue,
			Data: opaqueDB,
		},
	}
	colnamesRetList, err := convertToSlice(vzGetColNames(frame, colnamesArgs))
	if err != nil {
		return nil, err
	}
	if colnamesRetList[0].(bool) != true {
		return nil, fmt.Errorf("getting col names failed: %s", colnamesRetList[1].(string))
	}
	colnamesListVal, err := convertToSlice(funl.Value{Kind: funl.ListValue, Data: colnamesRetList[2]})
	if err != nil {
		return nil, err
	}
	colmap := make(map[string]*ColInfo)
	for _, cv := range colnamesListVal {
		colname := cv.(string)
		colArgs := []funl.Value{
			{
				Kind: funl.OpaqueValue,
				Data: opaqueDB,
			},
			{
				Kind: funl.StringValue,
				Data: colname,
			},
		}
		colRetList, err := convertToSlice(vzGetCol(frame, colArgs))
		if err != nil {
			return nil, err
		}
		if colRetList[0].(bool) != true {
			return nil, fmt.Errorf("getting col info failed: %s", colRetList[1].(string))
		}

		colInfo := &ColInfo{
			Name: colname,
			Col:  colRetList[2].(*fuvaluez.OpaqueCol),
		}
		colmap[colname] = colInfo
	}

	database := &Database{
		TopFrame: frame,
		Cols:     colmap,
		Db:       opaqueDB,
	}

	converterItem := &funl.Item{
		Type: funl.ValueItem,
		Data: funl.Value{
			Kind: funl.StringValue,
			Data: "proc(ast) import stdast _ _ r = call(stdast.eval-ast ast): r end",
		},
	}
	database.Converter = funl.HandleEvalOP(database.TopFrame, []*funl.Item{converterItem})

	return database, nil
}

type conf struct {
	m map[string]*string
}

func newConf() *conf {
	m := make(map[string]*string)
	m["valuport"] = flag.String("valuport", "9901", "port number")
	m["filepath"] = flag.String("filepath", "", "path to db-file location")
	m["dbname"] = flag.String("dbname", "valudb", "name of database")

	flag.Parse()

	newm := make(map[string]*string)
	for k, v := range m {
		envVal, found := os.LookupEnv(k)
		if found {
			newm[k] = &envVal
		} else {
			newm[k] = v
		}
	}

	return &conf{m: newm}
}

func (conf *conf) getVal(name string) string {
	return *(conf.m[name])
}

func main() {
	config := newConf()
	var err error

	// Initialize FunL standard libraries
	if err = std.InitSTD(); err != nil {
		panic(fmt.Errorf("Error in std-lib init (%v)", err))
	}
	if err = funl.InitFunSourceSTD(); err != nil {
		panic(fmt.Errorf("Error in std-lib (fun source) init (%v)", err))
	}

	frame := &funl.Frame{
		Syms:     funl.NewSymt(),
		OtherNS:  make(map[funl.SymID]funl.ImportInfo),
		Imported: make(map[funl.SymID]*funl.Frame),
	}
	frame.SetInProcCall(true)

	vzOpen = fuvaluez.GetVZOpen("open")
	vzClose = fuvaluez.GetVZClose("close")
	vzGetColNames = fuvaluez.GetVZGetColNames("get-col-names")
	vzGetCol = fuvaluez.GetVZGetCol("get-col")
	vzNewCol = fuvaluez.GetVZNewCol("new-col")
	vzPutValue = fuvaluez.GetVZPutValue("put-value")
	vzGetValues = fuvaluez.GetVZGetValues("get-values")
	vzTakeValues = fuvaluez.GetVZTakeValues("take-values")
	vzUpdate = fuvaluez.GetVZUpdate("update")

	db, err := OpenDatabase(frame, config)
	if err != nil {
		panic(err)
	}

	// Create server
	server := std.NewRServer(":" + config.getVal("valuport"))

	// Create RPC implementation
	putValueExtProc := funl.ExtProcType{Impl: getRPCPutValue(db)}
	putValueProcVal := funl.Value{Kind: funl.ExtProcValue, Data: putValueExtProc}

	getValuesExtProc := funl.ExtProcType{Impl: getRPCGetValues(db)}
	getValuesProcVal := funl.Value{Kind: funl.ExtProcValue, Data: getValuesExtProc}

	takeValuesExtProc := funl.ExtProcType{Impl: getRPCTakeValues(db)}
	takeValuesProcVal := funl.Value{Kind: funl.ExtProcValue, Data: takeValuesExtProc}

	updateExtProc := funl.ExtProcType{Impl: getRPCUpdate(db)}
	updateProcVal := funl.Value{Kind: funl.ExtProcValue, Data: updateExtProc}

	// And register it to server
	err = server.Register("put-value", putValueProcVal)
	if err != nil {
		fmt.Println("Err -> ", err)
		return
	}

	err = server.Register("get-values", getValuesProcVal)
	if err != nil {
		fmt.Println("Err -> ", err)
		return
	}

	err = server.Register("take-values", takeValuesProcVal)
	if err != nil {
		fmt.Println("Err -> ", err)
		return
	}

	err = server.Register("update", updateProcVal)
	if err != nil {
		fmt.Println("Err -> ", err)
		return
	}

	// signal handling
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println(fmt.Sprintf("Signal received: %v", sig))
		done <- true
	}()
	fmt.Println("running")
	<-done
	fmt.Println("closing db")
	db.Shutdown()
	fmt.Println("exiting")
}
