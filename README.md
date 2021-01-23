# valuserver
Server for [ValueZ](https://github.com/anssihalmeaho/fuvaluez) data store.
Provides basic services for updating and reading data to/from data store managed by server process.
Communication from client to server happens via [RPC](https://github.com/anssihalmeaho/funl/wiki/stdrpc).

Server provides access to value store for many client processes (services).
Clients are assumed to be implemented with [FunL](https://github.com/anssihalmeaho/funl) programming language.

There are also libraries (FunL modules) for clients to use:

- **valulib**: basic write and read operations
- **valuview**: for having local projection of data (cache maintained by library on background)

## Server process for value store
**valuserver** is server process for data store, it's implemented with Go.
There are several parameters that can be given to valuserver either as environment variables or as command-line options:

parameter | meaning | default
--------- | ------- | -------
VALUPORT | port number which is used for RPC | 9901
VALUFILEPATH | path to db-file location | ""
VALUDBNAME | database name | "valudb"

### Server usage
Server process is taken into use by cloning it, compiling and starting the process:

```
git clone https://github.com/anssihalmeaho/valuserver
cd valuserver
make
./valuserver
```

or in Windows (without Makefile):

```
git clone https://github.com/anssihalmeaho/valuserver
cd valuserver
go build -o valuserver.exe -v .
valuserver.exe
```

## Client libraries
Clients of valuserver use client libraries to access value store server.
Client can also use directly RPC to access valuserver if needed.
Functions given as arguments are changed to AST and serialized before
sending those in RPC.
**Note.** Functions are transferred as syntactical constructs so
function values (closures) may not work as scope of closure is not transferred.

### Client libraries usage
Client libraries are in FunL modules located in **clientlib** -directory.
Those are taken into use so that source files (__valulib.fnl__ and __valuview.fnl__)
are located in such directory that FunL interpreter can find those via **FUNLPATH** environment variable.
For more details see: https://github.com/anssihalmeaho/funl/wiki/Importing-modules

## Client library: valulib
valulib provides basic RPC services for writing to valuestore and reading from it.

### new-client
Creates client map. Client map is used when making requests to server.
Address of server is given as argument, for example: `'localhost:9901'`

```
call(valulib.new-client <address-string>) -> <client-map>
```

### put-value
Writes value to collection. 
Arguments are:

1. client map
2. collection name
3. value to be written

```
call(valulib.put-value <client-map> <col-name-string> <value>) -> list(<ok:bool> <error-string>)
```

### update
Updates collection items by applying function to items in collection.
Arguments are:

1. client map
2. collection name
3. function/procedure to be applied to items

```
call(valulib.update <client-map> <col-name-string> <update-function>) -> list(<ok:bool> <error-string> <was-any-updates:bool>)
```

### take-values
Takes items from collection which match with function given as argument.
Arguments are:

1. client map
2. collection name
3. function/procedure to match items

```
call(valulib.take-values <client-map> <col-name-string> <match-function>) -> list(<ok:bool> <error-string> <list:taken-values>)
```

### get-values
Reads items from collection which match with function given as argument.
Arguments are:

1. client map
2. collection name
3. function/procedure to match items

```
call(valulib.get-values <client-map> <col-name-string> <match-function>) -> list(<ok:bool> <error-string> <list:values>)
```

## valuview
valuview provides local projection of collection contents in client.
valuview library listens changes in valuserver and refreshes local cache in background.
(changes are listened in long-poll-wait RPC call and also periodically values are read)

### new-view
Creates view (map) to project collection contents filtered by function given as argument.
Arguments are:

1. address of server (string)
2. collection name (string)
3. filter function

```
call(valuview.new-view <address-string> <col-name-string> <filter-func>) -> view-map
```

### value
Reads value from local view (cache).

```
call(valuview.value <view-map>) -> list(<has-value:bool> <value>)
```

### del-view
Deletes view.

```
call(valuview.del-view <view-map>) -> string
```

## Example
Example of one client process writing values to valuserver and other that
is printing periodically values from its local view.

Start valuserver:
```
./valuserver

running
```

Writing values: **write_to_valuserver.fnl**:

```
ns main

import valulib

main = proc()
	import stdtime
	
	# create client map
	client = call(valulib.new-client 'localhost:9901')

	# put several values to collection
	_ = list(
		call(valulib.put-value client 'bands' 'New Order')
		call(valulib.put-value client 'bands' 'Joy Division')
		call(valulib.put-value client 'bands' 'Talking Heads')
	)

	# modify all values a bit
	_ = call(valulib.update client 'bands' func(x) list(true sprintf('Band: %s' x)) end)

	# wait a little
	_ = call(stdtime.sleep 3)
	
	# take two values from collection
	_ = call(valulib.take-values client 'bands' func(x) or(in(x 'New Order') in(x 'Joy Division')) end)

	# read all values remaining in collection and return those
	call(valulib.get-values client 'bands' func(x) true end)
end

endns
```

Viewing values: **viewer.fnl**:

```
ns main

import valuview

show-stuff-periodically = proc(client)
	call(proc()
		while( 
			call(proc()
				import stdtime

				has-value value = call(valuview.value client):
				_ = print('value -> ' list(has-value value))
				_ = call(stdtime.sleep 1)
				true
			end)
			'none'
		)
	end)
end

main = proc()
	client = call(valuview.new-view 'localhost:9901' 'bands' func(item) true end)
	call(show-stuff-periodically client)
end

endns
```

Start viewer:

```
 ./funla viewer.fnl
```

Run value writer:

```
./funla write_to_valuserver.fnl

list(true, '', list('Band: Talking Heads'))
```

Output of viewer:

```
value -> list(true, list())
value -> list(true, list())
value -> list(true, list('Band: New Order', 'Band: Joy Division', 'Band: Talking Heads'))
value -> list(true, list('Band: New Order', 'Band: Joy Division', 'Band: Talking Heads'))
value -> list(true, list('Band: New Order', 'Band: Joy Division', 'Band: Talking Heads'))
value -> list(true, list('Band: Talking Heads'))
value -> list(true, list('Band: Talking Heads'))
```

