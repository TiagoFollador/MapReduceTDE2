from mrjob.job import MRJob

# Descrição da commodity e custo da transação de menor valor por ano e categoria.

# country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category


class ValorMedio(MRJob):
    # Mapper
    # Chave: Nenhuma 
    # Valor: Cada linha do dataset de transações
    # Retorna: ((ano, categoria), (commodity, valor de comércio)) para cada linha válida
    def mapper(self, _, value):
        fields = value.split(';')
        if fields[0] == "country_or_area":
            return
        if len(fields) == 10: 
            year = fields[1]
            category = fields[9]
            commodity = fields[3]
            try: 
                tradeUsd = float(fields[5])
                yield (year, category), (commodity, tradeUsd)
            except ValueError:
                pass     

    # Combiner
    # Chave: (ano, categoria)
    # Valor: Uma lista de tuplas (commodity, valor de comércio) do mapper
    # Retorna: (ano, categoria), (commodity de menor valor, menor valor de comércio) até o momento
    def combiner(self, key, values):
        min_value = 0
        min_commodity  = 0
        
        for commodity, value in values:
             if min_value == 0 or value < min_value:
                min_value = value
                min_commodity = commodity
        yield key, (min_commodity, min_value)

    # Reducer
    # Chave: (ano, categoria)
    # Valor: Uma lista de tuplas (commodity, valor de comércio) do combiner
    # Retorna: (ano, categoria), (commodity de menor valor, menor valor de comércio) após consolidar todos os valores
    def reducer(self, key, values):
        min_value = 0
        min_commodity = 0
        
        for commodity, value in values:
            if min_value == 0 or value < min_value:
                min_value = value
                min_commodity = commodity
        
        yield key, (min_commodity, min_value)

if __name__ == "__main__":
    ValorMedio.run()

    
# python .\Atividade6\Atividade6.py .\operacoes_comerciais_inteira.csv --output .\Atividade6\output\
#  cat .\Atividade6\output\* > .\Atividade6\outputAtividade6
