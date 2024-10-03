from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class ValorMedioTransacao(MRJob):

    # Define os passos do mapper, combiner e reducer
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_filtrar_e_extrair,
                combiner=self.combiner_somar_e_contar,
                reducer=self.reducer_calcular_media
            )
        ]

    # Mapper
    # Chave: Nenhuma 
    # Valor: Cada linha do dataset de transações
    # Retorna: ((ano, categoria), (valor, 1)) para transações de exportação do Brasil
    def mapper_filtrar_e_extrair(self, _, linha):
        if linha.startswith('country_or_area'):
            return
        
        campos = linha.strip().split(";")
        
        if len(campos) < 10:
            return  

        pais = campos[0].strip()
        ano = campos[1].strip()
        categoria = campos[9].strip()  
        tipo_unidade = campos[7].strip()  
        fluxo = campos[4].strip()
        valor_str = campos[5].strip()

        if pais == "Brazil" and fluxo == "Export":
            try:
                valor = float(valor_str)
                yield (ano, categoria), (valor, 1)
            except ValueError:
                return

    # Combiner
    # Chave: (ano, categoria)
    # Valor: Uma lista de tuplas (valor, 1) do mapper
    # Retorna: (ano, categoria), (soma total dos valores, número total de transações)
    def combiner_somar_e_contar(self, chave, valores):
        valor_total = 0
        contador = 0
        for valor, cnt in valores:
            valor_total += valor
            contador += cnt
        yield chave, (valor_total, contador)

    # Reducer
    # Chave: (ano, categoria)
    # Valor: Uma lista de tuplas (soma total dos valores, número total de transações) do combiner
    # Retorna: JSON com (ano, categoria), e média dos valores de transações para exportações do Brasil
    def reducer_calcular_media(self, chave, valores):
        valor_total = 0
        contador_total = 0
        for valor, cnt in valores:
            valor_total += valor
            contador_total += cnt
        if contador_total > 0:
            media = valor_total / contador_total
            ano, categoria = chave
            chave_saida = [ano, categoria]  
            yield json.dumps(chave_saida), round(media, 2)

if __name__ == '__main__':
    ValorMedioTransacao.run()
