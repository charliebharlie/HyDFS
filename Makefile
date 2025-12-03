server: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server

client: ./cmd/client/main.go
	go build -o ./bin/client ./cmd/client

test: ./cmd/test/test.go
	go build -o ./bin/test ./cmd/test

plota: ./cmd/plota/main.go
	go build -o ./bin/plota ./cmd/plota

plotb: ./cmd/plotb/main.go
	go build -o ./bin/plotb ./cmd/plotb

plotc: ./cmd/plotc/main.go
	go build -o ./bin/plotc ./cmd/plotc
	
start: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server
	ansible-playbook -i inventory.ini install-playbook.yml

deploy: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server
	go build -o ./bin/client ./cmd/client
	ansible-playbook -i inventory.ini fast_redeploy.yml

clean:
	rm -f ./bin/client
	rm -f ./bin/server
	rm -f ./bin/plota
	rm -f ./bin/plotb
	rm -f ./bin/plotc
