from mrjob.job import MRJob
from mrjob.step import MRStep

class CommoditiesPorFluxo(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_filtrar_transacoes,
                   combiner=self.combiner_agregar,
                   reducer=self.reducer_encontrar_maximo)
        ]

    def mapper_filtrar_transacoes(self, _, linha):
        colunas = linha.split(';')

        if colunas[0] == "country_or_area":
            return

        pais = colunas[0].strip()
        ano = int(colunas[1].strip())
        mercadoria = colunas[3].strip()
        fluxo = colunas[4].strip()
        nome_quantidade = colunas[7].strip()
        quantidade = colunas[8].strip()

        if pais == 'China' and ano == 2014 and nome_quantidade == 'Number of items':
            yield (fluxo, mercadoria), int(quantidade)

    def combiner_agregar(self, chave, valores):
        yield chave, sum(valores)

    def reducer_encontrar_maximo(self, chave, valores):

        fluxo, mercadoria = chave
        quantidade_total = sum(valores)
        yield fluxo, (mercadoria, quantidade_total)

if __name__ == '__main__':
    CommoditiesPorFluxo.run()

# python .\Atividade7\Atividade7.py .\operacoes_comerciais_inteira.csv --output .\Atividade7\output\
# cat .\Atividade7\output\* > .\Atividade7\outputAtividade7