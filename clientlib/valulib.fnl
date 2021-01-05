
ns valulib

import stddbc
import stdrpc
import stdast

new-client = proc(address)
	_ = call(stddbc.assert eq(type(address) 'string') 'address not a string')
	proxy = call(stdrpc.new-proxy address)
	map(
		'addr'  address
		'proxy' proxy
	)
end

put-value = proc(client col-name value)
	proxy = get(client 'proxy')
	ok err rval = call(stdrpc.rcall proxy 'put-value' col-name value):
	if( ok
		rval
		list(ok err)
	)
end

get-service = func(service-name)
	proc(client col-name filter)
		proxy = get(client 'proxy')

		ast-ok ast-err ast = call(stdast.func-value-to-ast filter):
		if( ast-ok
			call(stdrpc.rcall proxy service-name col-name ast)
			list(ast-ok ast-err ast)
		)
	end
end

take-values = call(get-service 'take-values')
update = call(get-service 'update')
get-values = call(get-service 'get-values')

endns

