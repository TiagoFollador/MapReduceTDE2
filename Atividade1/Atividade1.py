from mrjob.job import MRJob
from mrjob.step import MRStep

class ContagemTransacoes(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_obter_pais,
                   combiner=self.combiner_contar_transacoes,
                   reducer=self.reducer_contar_transacoes)
        ]
    
    def mapper_obter_pais(self, _, linha):
        if linha.startswith('country_or_area'):
            return
        
        campos = linha.split(";")
        
        pais = campos[0]
        
        if pais in ["Australia", "Brazil", "China"]:
            yield (pais, 1)

    def combiner_contar_transacoes(self, pais, contagens):
        yield (pais, sum(contagens))

    def reducer_contar_transacoes(self, pais, contagens):
        yield (pais, sum(contagens))

if __name__ == '__main__':
    ContagemTransacoes.run()

# python .\Atividade1\Atividade1.py .\operacoes_comerciais_inteira.csv --output .\Atividade1\output\
# cat .\Atividade1\output\* > .\Atividade1\outputAtividade1