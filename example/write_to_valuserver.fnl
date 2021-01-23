
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

