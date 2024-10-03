from mrjob.job import MRJob
from mrjob.step import MRStep

class ContagemTransacoes(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_obter_pais,
                   combiner=self.combiner_contar_transacoes,
                   reducer=self.reducer_contar_transacoes)
        ]

    #Mapper
    #Chave: Nenhuma
    #Valor: Cada linha do dataset de transações
    #Retorna: (país, 1) se o país for "Australia", "Brazil" ou "China"
    def mapper_obter_pais(self, _, linha):
        if linha.startswith('country_or_area'):
            return
        
        campos = linha.split(";")
        
        pais = campos[0]
        
        if pais in ["Australia", "Brazil", "China"]:
            yield (pais, 1)

    #Combiner
    #Chave: país
    #Valor: Uma lista de contagens de 1s do mapper
    #Retorna: (país, soma das contagens)
    def combiner_contar_transacoes(self, pais, contagens):
        yield (pais, sum(contagens))

    #Reducer
    #Chave: país
    #Valor: Uma lista de contagens parciais do combiner
    #Retorna: (país, soma total das contagens)
    def reducer_contar_transacoes(self, pais, contagens):
        yield (pais, sum(contagens))

if __name__ == '__main__':
    ContagemTransacoes.run()


# python .\Atividade1\Atividade1.py .\operacoes_comerciais_inteira.csv --output .\Atividade1\output\
# cat .\Atividade1\output\* > .\Atividade1\outputAtividade1
