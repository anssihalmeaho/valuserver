
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

