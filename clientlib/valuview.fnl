
ns valuview

client-viewer = proc(client-ch notif-ch del-client-ch del-listener-ch)
	client-handler = proc(content replych)
		_ = send(replych content)
		content
	end

	shutdown = proc(replych)
		_ = send(del-listener-ch 'time to leave')
		#_ = print('done: client viewer')
		send(replych 'del done')
	end

	poller = proc(content)
		while( true
			call(proc()
				select(
					client-ch     proc(replych) call(client-handler content replych) end
					notif-ch      proc(new-value) new-value end
					del-client-ch proc(replych) _ = call(shutdown replych) error('exiting') end
				)
			end)
			'none'
		)
	end

	try(call(poller list(false '')))
end

listener = proc(col-name notif-ch client selector del-listener-ch)
	import valulib
	import stdtime
	import stdrpc

	proxy = get(client 'proxy')

	poller = proc(notif-info)
		while( true
			call(proc()
				# check if needs to exit
				got-del _ = recwith(del-listener-ch map('wait' false)):
				#_ = if(got-del print('done: listener') '')
				_ = if(got-del error(listener exit) '')

				# read values
				ok err val = call(valulib.get-values client col-name selector):
				_ = if(ok send(notif-ch list(true val)) 'none')

				# long poll waiting
				ok2 err2 sublist = call(stdrpc.rcall proxy 'valu-long-waiter' notif-info):
				subl = if( eq(type(sublist) 'list') sublist list(false 'failure' list()))
				sub-ok sub-err notif-data = subl:
				next-version = if(ok2 get(notif-data 'version') get(notif-info 'version'))

				map(
					'is-init'       false
					'version'       next-version
					'col'           col-name
					'wait-time-sec' 10
				)
			end)
			'none'
		)
	end

	notif-subs = map(
		'is-init'       true
		'version'       ''
		'col'           col-name
		'wait-time-sec' 10
	)

	try(call(poller notif-subs))
end

new-view = proc(addr col-name selector)
	import stdast
	import valulib

	client = call(valulib.new-client addr)

	client-ch = chan()
	notif-ch = chan()
	del-client-ch = chan()
	del-listener-ch = chan()

	_ = spawn(call(client-viewer client-ch notif-ch del-client-ch del-listener-ch))
	_ = spawn(call(listener col-name notif-ch client selector del-listener-ch))

	map(
		'colname'       col-name
		'stopped'       false
		'ch'            client-ch
		'del-client-ch' del-client-ch
	)
end

value = proc(client)
	replych = chan()
	_ = send(get(client 'ch') replych)
	recv(replych)
end

del-view = proc(client)
	replych = chan()
	_ = send(get(client 'del-client-ch') replych)
	recv(replych)
end

endns
