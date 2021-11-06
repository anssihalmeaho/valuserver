package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/anssihalmeaho/funl/funl"
	"github.com/anssihalmeaho/funl/std"

	"github.com/anssihalmeaho/fuvaluez/fuvaluez"
)

// ListenerInfo ...
type ListenerInfo struct {
	IsInit        bool
	Version       string
	ColName       string
	WaitTimeInSec int
}

func getListener(frame *funl.Frame, mapv funl.Value) (listener ListenerInfo, err error) {
	keyvals := funl.HandleKeyvalsOP(frame, []*funl.Item{&funl.Item{Type: funl.ValueItem, Data: mapv}})
	kvListIter := funl.NewListIterator(keyvals)
	for {
		nextKV := kvListIter.Next()
		if nextKV == nil {
			break
		}
		kvIter := funl.NewListIterator(*nextKV)
		keyv := *(kvIter.Next())
		valv := *(kvIter.Next())
		if keyv.Kind != funl.StringValue {
			err = fmt.Errorf("key not a string: %v", keyv)
			return
		}
		switch keyStr := keyv.Data.(string); keyStr {
		case "is-init":
			if valv.Kind != funl.BoolValue {
				err = fmt.Errorf("%s invalid value: %v", keyStr, keyv)
				return
			}
			listener.IsInit = valv.Data.(bool)
		case "version":
			if valv.Kind != funl.StringValue {
				err = fmt.Errorf("%s invalid value: %v", keyStr, keyv)
				return
			}
			listener.Version = valv.Data.(string)
		case "col":
			if valv.Kind != funl.StringValue {
				err = fmt.Errorf("%s invalid value: %v", keyStr, keyv)
				return
			}
			listener.ColName = valv.Data.(string)
		case "wait-time-sec":
			if valv.Kind != funl.IntValue {
				err = fmt.Errorf("%s invalid value: %v", keyStr, keyv)
				return
			}
			listener.WaitTimeInSec = valv.Data.(int)
		}
	}
	return
}

func getLongWaiter(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	makeErrRetList := func(frame *funl.Frame, errtxt string) funl.Value {
		rvals := []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: false,
			},
			{
				Kind: funl.StringValue,
				Data: errtxt,
			},
			{
				Kind: funl.StringValue,
				Data: "",
			},
		}
		return funl.MakeListOfValues(frame, rvals)
	}

	rcpLongWaiterValue := func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		if len(arguments) == 0 {
			retVal = makeErrRetList(frame, "no arguments received")
			return
		}
		listener, err := getListener(frame, arguments[0])
		if err != nil {
			retVal = makeErrRetList(frame, err.Error())
			return
		}

		notifCh := make(chan UpdateInfo)
		var prevVers string
		var newVers string
		var isTimeout bool
		if !listener.IsInit {
			prevVers = listener.Version
		}
		notifInfo := NotifInfo{
			ColName:         listener.ColName,
			Ch:              notifCh,
			PreviousVersion: prevVers,
		}
		db.ListenAddCh <- notifInfo
		select {
		case notif := <-notifCh:
			//fmt.Println("notif -> ", notif)
			newVers = notif.Version
		case <-time.After(time.Duration(listener.WaitTimeInSec) * time.Second):
			//fmt.Println("timeout")
			newVers = prevVers
			isTimeout = true
		}
		db.ListenDelCh <- notifInfo

		operands := []*funl.Item{
			&funl.Item{
				Type: funl.ValueItem,
				Data: funl.Value{
					Kind: funl.StringValue,
					Data: "version",
				},
			},
			&funl.Item{
				Type: funl.ValueItem,
				Data: funl.Value{
					Kind: funl.StringValue,
					Data: newVers,
				},
			},
			&funl.Item{
				Type: funl.ValueItem,
				Data: funl.Value{
					Kind: funl.StringValue,
					Data: "was-timedout",
				},
			},
			&funl.Item{
				Type: funl.ValueItem,
				Data: funl.Value{
					Kind: funl.BoolValue,
					Data: isTimeout,
				},
			},
		}
		mapv := funl.HandleMapOP(frame, operands)

		rvals := []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: true,
			},
			{
				Kind: funl.StringValue,
				Data: "",
			},
			mapv,
		}
		retVal = funl.MakeListOfValues(frame, rvals)
		return
	}
	return rcpLongWaiterValue
}

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

		// notify about change too
		db.UpdateCh <- UpdateInfo{ColName: colName}
		return
	}
	return rpcPutValue
}

func getRPChandler(db *Database, vzHandler fuvaluez.FZProc, doNotify func(funl.Value) bool) func(*funl.Frame, []funl.Value) funl.Value {
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

		// notify about change too
		if doNotify(retVal) {
			// TODO: it could be checked whether anything was updated...
			db.UpdateCh <- UpdateInfo{ColName: colName}
		}
		return
	}
	return rpcHandler
}

func getRPCTakeValues(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	// if no values were taken then no need to notify change
	doNotify := func(val funl.Value) bool {
		lit := funl.NewListIterator(val)
		if lit.Next() == nil {
			return false
		}
		return true
	}
	return getRPChandler(db, vzTakeValues, doNotify)
}

func getRPCUpdate(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	// if no values updated then no need to notify change
	doNotify := func(val funl.Value) bool {
		return val.Kind == funl.BoolValue && val.Data.(bool)
	}
	return getRPChandler(db, vzUpdate, doNotify)
}

func getRPCGetValues(db *Database) func(*funl.Frame, []funl.Value) funl.Value {
	// this doesnt change values so no change notify
	doNotify := func(val funl.Value) bool {
		return false
	}
	return getRPChandler(db, vzGetValues, doNotify)
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

// UpdateInfo ...
type UpdateInfo struct {
	ColName string
	Version string
}

// Database represents valuez db
type Database struct {
	colsLock    sync.RWMutex
	TopFrame    *funl.Frame
	Cols        map[string]*ColInfo
	Db          *fuvaluez.OpaqueDB
	Converter   funl.Value
	UpdateCh    chan UpdateInfo
	ListenAddCh chan NotifInfo
	ListenDelCh chan NotifInfo
}

// NotifInfo ...
type NotifInfo struct {
	ColName         string
	Ch              chan UpdateInfo
	PreviousVersion string // if "" then there is no previous
}

// Notifier ...
func (db *Database) Notifier() {
	versions := make(map[string]int)
	var listeners = map[string]map[chan UpdateInfo]NotifInfo{}

	for {
		select {
		case upd := <-db.UpdateCh:
			colVers, colFound := versions[upd.ColName]
			if colFound {
				versions[upd.ColName] = colVers + 1
			} else {
				versions[upd.ColName] = 20
			}
			//fmt.Println("upd -> ", versions[upd.ColName])
			targetm, found := listeners[upd.ColName]
			if found {
				upd.Version = strconv.Itoa(versions[upd.ColName])
				for targetCh := range targetm {
					select {
					case targetCh <- upd:
					default:
					}
				}
			}

		case subs := <-db.ListenAddCh:
			//fmt.Println("add -> ", subs)
			if subs.ColName == "" {
				close(subs.Ch)
			}

			// lets check if version is different, if so then trigger notification
			// rightaway
			if subs.PreviousVersion != "" {
				currentVersion := strconv.Itoa(versions[subs.ColName])
				if subs.PreviousVersion != currentVersion {
					//fmt.Println("Immediate upd: ", currentVersion)

					upd := UpdateInfo{
						ColName: subs.ColName,
						Version: currentVersion,
					}
					select {
					case subs.Ch <- upd:
					default:
					}
				}
			}

			_, found := listeners[subs.ColName]
			if !found {
				newm := make(map[chan UpdateInfo]NotifInfo)
				listeners[subs.ColName] = newm
			}
			listeners[subs.ColName][subs.Ch] = subs

		case unsubs := <-db.ListenDelCh:
			//fmt.Println("del -> ", unsubs)
			subs, found := listeners[unsubs.ColName][unsubs.Ch]
			if found {
				delete(listeners[unsubs.ColName], subs.Ch)
				close(subs.Ch)
			}
		}
	}
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

	// there might be possibility for collision in creating col
	// BUT situation recovers after a while, no error is caused for client
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
			Data: config.getVal("VALUFILEPATH") + config.getVal("VALUDBNAME"),
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

	// lets create notifier
	database.UpdateCh = make(chan UpdateInfo, 1000)
	database.ListenAddCh = make(chan NotifInfo)
	database.ListenDelCh = make(chan NotifInfo)
	go database.Notifier()

	return database, nil
}

type conf struct {
	m map[string]*string
}

func newConf() *conf {
	m := make(map[string]*string)
	m["VALUPORT"] = flag.String("VALUPORT", "9901", "port number")
	m["VALUFILEPATH"] = flag.String("VALUFILEPATH", "", "path to db-file location")
	m["VALUDBNAME"] = flag.String("VALUDBNAME", "valudb", "name of database")

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

	interpreter := funl.NewInterpreter()

	// Initialize FunL standard libraries
	if err = std.InitSTD(interpreter); err != nil {
		panic(fmt.Errorf("Error in std-lib init (%v)", err))
	}
	if err = funl.InitFunSourceSTD(interpreter); err != nil {
		panic(fmt.Errorf("Error in std-lib (fun source) init (%v)", err))
	}

	frame := funl.NewTopFrameWithInterpreter(interpreter)
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
	server := std.NewRServer(frame, ":"+config.getVal("VALUPORT"))

	// Create RPC implementation
	putValueExtProc := funl.ExtProcType{Impl: getRPCPutValue(db)}
	putValueProcVal := funl.Value{Kind: funl.ExtProcValue, Data: putValueExtProc}

	getValuesExtProc := funl.ExtProcType{Impl: getRPCGetValues(db)}
	getValuesProcVal := funl.Value{Kind: funl.ExtProcValue, Data: getValuesExtProc}

	takeValuesExtProc := funl.ExtProcType{Impl: getRPCTakeValues(db)}
	takeValuesProcVal := funl.Value{Kind: funl.ExtProcValue, Data: takeValuesExtProc}

	updateExtProc := funl.ExtProcType{Impl: getRPCUpdate(db)}
	updateProcVal := funl.Value{Kind: funl.ExtProcValue, Data: updateExtProc}

	longWaiterExtProc := funl.ExtProcType{Impl: getLongWaiter(db)}
	longWaiterProcVal := funl.Value{Kind: funl.ExtProcValue, Data: longWaiterExtProc}

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

	err = server.Register("valu-long-waiter", longWaiterProcVal)
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
