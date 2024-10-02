from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class ValorMedioTransacao(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_filtrar_e_extrair,
                combiner=self.combiner_somar_e_contar,
                reducer=self.reducer_calcular_media
            )
        ]

    def mapper_filtrar_e_extrair(self, _, linha):
        if linha.startswith('country_or_area'):
            return
        
        campos = linha.strip().split(";")
        
        if len(campos) < 6:
            return  

        pais = campos[0].strip()
        ano = campos[1].strip()
        categoria = campos[2].strip()
        tipo_unidade = campos[3].strip()  
        fluxo = campos[4].strip()
        valor_str = campos[5].strip()

        if pais == "Brazil" and fluxo == "Export":
            try:
                valor = float(valor_str)
                yield (ano, categoria), (valor, 1)
            except ValueError:
                return

    def combiner_somar_e_contar(self, chave, valores):
        valor_total = 0
        contador = 0
        for valor, cnt in valores:
            valor_total += valor
            contador += cnt
        yield chave, (valor_total, contador)

    def reducer_calcular_media(self, chave, valores):
        valor_total = 0
        contador_total = 0
        for valor, cnt in valores:
            valor_total += valor
            contador_total += cnt
        if contador_total > 0:
            media = valor_total / contador_total
            ano, categoria = chave
            chave_saida = ["Brazil", ano, categoria]
            yield json.dumps(chave_saida), round(media, 2)

if __name__ == '__main__':
    ValorMedioTransacao.run()
