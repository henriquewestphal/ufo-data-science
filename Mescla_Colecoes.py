# Bibliotecas
import Util
import re

# Temos duas coleções que precisam ser mescladas: ufo.ufos e dbclima.clima
# Para cada documento em dbclima.clima, vamos localizar cidade, estado, ano, mes, dia, hora e minuto em ufos.ufo
# Caso tenhamos um "match", um documento, mais enxuto, será gravado em uma terceira coleção

# Conexões
cliente_MongoDB_ufos = Util.fnc_Conecta_Base_Documentos('', '', 'localhost', '27017', 'ufos')
db_ufos = cliente_MongoDB_ufos.ufos

cliente_MongoDB_db_clima = Util.fnc_Conecta_Base_Documentos('', '', 'localhost', '27017', 'dbclima')
db_clima = cliente_MongoDB_db_clima.dbclima

# Localiza maior posicao gravada na coleção clima_consolidado
consulta_ultimo_armazenado = db_clima.clima_consolidado.find_one (sort=[("posicao", -1)])
ultimo_carregado = 0
if 'posicao' in consulta_ultimo_armazenado:
    ultimo_carregado = consulta_ultimo_armazenado['posicao']

# Cria view sobre coleção clima, mas apenas para posições maiores à encontrada
db_clima.vclima.drop()
pipeline = [{"$match": { "posicao" : { "$gt" : ultimo_carregado } } }]
db_clima.command({
    "create": "vclima",
    "viewOn": "clima",
    "pipeline": pipeline
})

# Cria lista para armazenar _id de UFOs, cujos dados climáticos já foram encontrados
lista_ufos_encontrados = []

# Visitando todo documento de ufo (não seria necessário explicitar campos)
for u in db_ufos.ufo.find({}, { "_id" : 1, "City" : 1, "State" : 1, "Shape" : 1,
                                 "Sight_Year" : 1,"Sight_Month" : 1, "Sight_Day" : 1,
                                 "Sight_Time": 1}):
    cidade = u["City"]
    estado = u["State"]
    # Tratamento do ano. Teremos problemas em 2090!
    # Ideal seria mudar a fonte para que o ano venha com 4 dígitos
    ano = int(u["Sight_Year"])
    if ano > 90:
        ano = ano + 1900
    else:
        ano = ano + 2000
    ano = str(ano)

    mes = u["Sight_Month"].zfill(2)
    dia = u["Sight_Day"].zfill(2)
    # Esmiuçando "Sight_Time" em: hora e minuto. Exemplo  00:05
    # Temos que garantir o tamanho 2 para cada item. Exemplo: se ler hora "8", precisamos transformá-lo em "08"
    x = re.findall('\d+', u["Sight_Time"]) # Extraímos os 2 números
    hora = x[0].zfill(2)
    #minuto = x[1].zfill(2)
    # Consulta
    pipeline = [
            {"$match": { "estado" : estado, "cidade" : cidade }},
            {"$unwind": "$history.observations"},
            {"$match": {"history.observations.date.mon": mes,
                        "history.observations.date.mday": dia,
                        "history.observations.date.year": ano,
                        #"history.observations.date.hour": hora,
                        #"history.observations.date.min": minuto
                 }},
            {"$project": {"_id": 0,
                          "posicao" : 1,
                          "history.observations.tempi" : 1,
                          "history.observations.tempm" : 1,
                          "history.observations.dewptm" :1,
                          "history.observations.dewpti" : 1,
                          "history.observations.hum" : 1,
                          "history.observations.wspdm" : 1,
                          "history.observations.wspdi": 1,
                          "history.observations.wgustm": 1,
                          "history.observations.wgusti": 1,
                          "history.observations.wdird": 1,
                          "history.observations.wdire": 1,
                          "history.observations.vism": 1,
                          "history.observations.visi": 1,
                          "history.observations.pressurem": 1,
                          "history.observations.pressurei": 1,
                          "history.observations.windchillm": 1,
                          "history.observations.windchilli": 1,
                          "history.observations.heatindexm": 1,
                          "history.observations.heatindexi": 1,
                          "history.observations.precipm": 1,
                          "history.observations.precipi": 1,
                          "history.observations.conds": 1,
                          "history.observations.icon": 1,
                          "history.observations.fog": 1,
                          "history.observations.rain": 1,
                          "history.observations.snow": 1,
                          "history.observations.hail": 1,
                          "history.observations.thunder": 1,
                          "history.observations.tornado": 1
                          }}
                    ]
    busca_medida = db_clima.vclima.aggregate(pipeline)
    medidas = next(busca_medida, None)
    if medidas:
        # Extrai a posicao:
        posicao = medidas["posicao"]
        # Busca posicao em clima_consolidado
        acha_posicao = db_clima.clima_consolidado.find({"posicao": posicao})
        if (acha_posicao.count() == 0):  # não achou
            # Se não achar, monta cabeçalho e procede com gravação
            cabecalho = {
                'posicao': posicao,
                'estado': estado,
                'cidade': cidade,
                'formato': u["Shape"],
                'dia': dia,
                'mes': mes,
                'ano': ano,
                #'hora': hora
                #  'minuto': minuto
            }
            # Juntando ambas as estruturas (dictionaries):
            json_para_gravar = {**cabecalho, **medidas}
            try:
                # Inserindo documento na coleção "clima"
                resultado = db_clima.clima_consolidado.insert_one(json_para_gravar)
                lista_ufos_encontrados.append (u["_id"])
                print (cidade, estado, ano, mes, dia)
            except:
                print("Provavelmente tentativa de inseção duplicada: ", cidade, estado, ano, mes, dia)
        else:
            print("Documento existente!")
    else:
        print ("Não encontrou documento em vclima! ")
# Elimina na coleção de UFOs os "_id" cujos dados climáticos correspondentes foram encontrados
for id in lista_ufos_encontrados:
    db_ufos.ufo.delete_one ({ '_id' : id })

# Fim




