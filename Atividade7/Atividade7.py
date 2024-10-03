from mrjob.job import MRJob
from mrjob.step import MRStep

class CommoditiesPorFluxo(MRJob):

    # Define os passos do mapper, combiner e dois reducers
    def steps(self):
        return [
            MRStep(mapper=self.mapper_filtrar_transacoes,
                   combiner=self.combiner_agregar,
                   reducer=self.reducer_encontrar_maximo_fluxo),
            MRStep(reducer=self.reducer_encontrar_maximo_por_fluxo)
        ]

    # Mapper
    # Chave: Nenhuma 
    # Valor: Cada linha do dataset de transações
    # Retorna: ((fluxo, mercadoria), quantidade) para transações da China em 2014, filtradas por 'Number of items'
    def mapper_filtrar_transacoes(self, _, linha):
        colunas = linha.split(';')

        if colunas[0] == "country_or_area":
            return

        try:
            pais = colunas[0].strip()
            ano = int(colunas[1].strip())
            mercadoria = colunas[3].strip()
            fluxo = colunas[4].strip()
            nome_quantidade = colunas[7].strip()
            quantidade = int(colunas[8].strip())
        except ValueError:
            return

        if pais == 'China' and ano == 2014 and nome_quantidade == 'Number of items':
            yield (fluxo, mercadoria), quantidade

    # Combiner
    # Chave: (fluxo, mercadoria)
    # Valor: Uma lista de quantidades do mapper
    # Retorna: (fluxo, mercadoria), soma das quantidades
    def combiner_agregar(self, chave, valores):
        yield chave, sum(valores)

    # Reducer 1
    # Chave: (fluxo, mercadoria)
    # Valor: Soma das quantidades do combiner
    # Retorna: fluxo, (mercadoria, quantidade total)
    def reducer_encontrar_maximo_fluxo(self, chave, valores):
        fluxo, mercadoria = chave
        quantidade_total = sum(valores)

        yield fluxo, (mercadoria, quantidade_total)

    # Reducer 2
    # Chave: fluxo
    # Valor: Uma lista de tuplas (mercadoria, quantidade total) do primeiro reducer
    # Retorna: fluxo, (mercadoria com maior quantidade, maior quantidade)
    def reducer_encontrar_maximo_por_fluxo(self, fluxo, mercadorias_quantidades):
        max_mercadoria, max_quantidade = None, 0

        for mercadoria, quantidade in mercadorias_quantidades:
            if quantidade > max_quantidade:
                max_mercadoria, max_quantidade = mercadoria, quantidade

        yield fluxo, (max_mercadoria, max_quantidade)

if __name__ == '__main__':
    CommoditiesPorFluxo.run()
