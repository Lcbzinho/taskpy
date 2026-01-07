# multithreading

Código com o intuito educacional.

Execução desse código permite o web scrapping do site do IMDB, armazenando título de filmes, ano de lançamento e notas recebidas.

Detalhe: Como o IMDB efetua alteração de sua estrutura constantemente, é necessário a mudança das classes periodicamente. Caso o código não esteja funcionando, abrir um pedido de atualização, farei o mais breve possível!

## Versão assíncrona (asyncio + aiohttp)

Para um exemplo moderno e eficiente usando asyncio, adicionamos `async_scraper.py`, que permite coletar páginas de forma concorrente (sem threads) e extrair dados via seletores CSS.

### Instalação

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Uso básico

- Uma URL, sem seletores (extrai `title` e todos os `h1` por padrão):

```powershell
python async_scraper.py --url https://quotes.toscrape.com/
```

- Várias URLs e seletores nomeados (formato `nome=css[@attr]`):

```powershell
python async_scraper.py \
	--url https://quotes.toscrape.com/ \
	--url https://quotes.toscrape.com/page/2/ \
	--selector quote=.quote span.text \
	--selector author=.quote small.author \
	--selector link=.quote a@href \
	--concurrency 10 \
	--delay 0.5 \
	--per-host \
	--output resultados.jsonl
```

- Arquivo com URLs (uma por linha):

```powershell
python async_scraper.py --urls-file urls.txt --selector title=title
```

Saída padrão mostra um resumo por URL e, se `--output` for informado, os resultados completos são gravados em JSON Lines.

### Observações
- Use com responsabilidade. Respeite `robots.txt`, termos de uso e limites do site.
- Ajuste `--concurrency` para evitar sobrecarregar servidores.
- O cabeçalho `User-Agent` padrão é definido para uso educacional.

### Limite de taxa (Rate limit)
### Limite de taxa (Rate limit)
- `--delay`: atraso mínimo entre requisições (segundos).
- `--per-host`: aplica o atraso por host (em vez de global).

### Formatos de saída e robôs
- `--output` + `--output-format jsonl|csv`: grava resultados em JSON Lines ou CSV.
- `--verbose`: logs informativos sobre progresso e bloqueios por robots.
- `--respect-robots`: respeita `robots.txt` antes da coleta.

## IMDB (versão assíncrona CSV)

Scraper dedicado para o ranking "Most Popular" do IMDB usando asyncio: [async_imdb.py](async_imdb.py)

Exemplo:

```powershell
python async_imdb.py --concurrency 10 --delay 0.5 --output movies_async.csv
	--output resultados.csv `
	--output-format csv `
	--verbose `
	--respect-robots

Saída: CSV com colunas `title,date,rating,plot`.

## Arquivo de exemplos
- [urls.txt](urls.txt): duas URLs do site de testes `quotes.toscrape.com`.
