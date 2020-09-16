# Rotating Proxy Server

## API calls

`/get-random?scraper=<name_of>`

`/inc-good-attempts?scraper=<name_of>&proxy=<proxy_address>`

`/mark-dead?scraper=<name_of>&proxy=<proxy_address>`

### Special cases

`/reload-proxy-list`

`/add-proxies` [POST] json `{'scraper': name, 'proxies': ['proxy', 'list']}`

`/remove-proxies` [POST] json `{'scraper': name, 'proxies': ['proxy', 'list']}`

`/reanimate?scraper=<name_of>`

`/remove-dead?days=<number of days, int to to remove too long dead proxies from the database>` (default = 30)

`/alive-from-dead?scraper=<name_of>`

`/max-good-attempts?numbers=<numbers of successful tries>`

`/get-working-list?scraper=<name_of>`

`/get-dead-list?scraper=<name_of>`

`/get-proxy-usefulness-stats?scraper=<name_of>&orderby=[name, sdate, success, fdate, fail]`

`/clear-usefulness-stats`

### Scraper/spider names

`ra` - for ryanair spider

`wizz` - for wizzair spider

### Local run

local env:

`pmserver=development`

`pmserver_local_run=local`

In console

`./run.sh`

go to 127.0.0.1/5000
