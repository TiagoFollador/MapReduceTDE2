from mrjob.job import MRJob

# Número de transações por ano

class Transacoes(MRJob):

    # Mapper
    #Chave: Nenhuma 
    #Valor: Cada linha do dataset de transações
    #Retorna: (ano, 1) se a linha não for o cabeçalho e tiver 10 campos
    def mapper(self, _, value):
        fields = value.split(';')
        if fields[0] == "country_or_area":
            return
        if len(fields) == 10:  # ignorar cabeçalho
            year = fields[1]
            yield year, 1

    # Combiner
    #Chave: ano
    #Valor: Uma lista de contagens (1s) do mapper
    #Retorna: (ano, soma das contagens)
    def combiner(self, key, value):
        yield key, sum(value)

    # Reducer
    #Chave: ano
    #Valor: Uma lista de contagens parciais do combiner
    #Retorna: (ano, soma total das contagens)
    def reducer(self, key, value):
        yield key, sum(value)


if __name__ == "__main__":
    Transacoes.run()

    
# python .\Atividade2\Atividade2.py .\operacoes_comerciais_inteira.csv --output .\Atividade2\output\
# cat .\Atividade2\output\* > .\Atividade2\output_completo
