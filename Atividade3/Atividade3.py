from mrjob.job import MRJob
from mrjob.step import MRStep

class ContagemTransacoesFluxoAno(MRJob):

    # Definição dos passos do mapper, combiner, reducer
    def steps(self):
        return [
            MRStep(mapper=self.mapper_obter_fluxo_ano,
                   combiner=self.combiner_contar_transacoes,
                   reducer=self.reducer_contar_transacoes)
        ]
    
    # Mapper
    # Chave: Nenhuma (identificador de chave é ignorado)
    # Valor: Cada linha do dataset de transações
    # Retorna: ((fluxo, ano), 1) para cada linha que contém dados válidos
    def mapper_obter_fluxo_ano(self, _, linha):
        if linha.startswith('country_or_area'):
            return
        campos = linha.split(";")
        
        ano = campos[1]
        fluxo = campos[4]  
        yield ((fluxo, ano), 1)

    
    # Combiner
    # Chave: (fluxo, ano)
    # Valor: Uma lista de contagens (1s) do mapper
    # Retorna: ((fluxo, ano), soma das contagens)
    def combiner_contar_transacoes(self, fluxo_ano, contagens):
        yield (fluxo_ano, sum(contagens))

    
    # Reducer
    # Chave: (fluxo, ano)
    # Valor: Uma lista de contagens parciais do combiner
    # Retorna: ((fluxo, ano), soma total das contagens)
    def reducer_contar_transacoes(self, fluxo_ano, contagens):
        yield (fluxo_ano, sum(contagens))

if __name__ == '__main__':
    ContagemTransacoesFluxoAno.run()


# python .\Atividade3\Atividade3.py .\operacoes_comerciais_inteira.csv --output .\Atividade3\output\
# cat .\Atividade3\output\* > .\Atividade3\outputAtividade3
