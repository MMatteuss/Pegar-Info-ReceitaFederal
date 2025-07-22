# 📊 Projeto Telecom CNPJ Brasil

Este repositório contém um projeto Python desenvolvido para **baixar**, **descompactar**, **dividir**, **tratar**, **validar** e **inserir** grandes bases de dados de empresas brasileiras (dados abertos da Receita Federal) em um banco de dados MySQL.

O objetivo principal é **automatizar** o processo de importação massiva de dados de empresas para sistemas de Telecom, facilitando consultas e geração de leads.

---

## ⚙️ Funcionalidades

- **Download Automático:** Busca os arquivos ZIP mais recentes da Receita Federal.
- **Descompactação:** Extrai os CSVs de cada arquivo ZIP.
- **Divisão de Arquivos:** Divide grandes CSVs em partes menores para facilitar a carga no banco.
- **Criação e Gerenciamento do Banco:** Cria ou recria as tabelas principais (`empresas`, `estabelecimentos`, `socios`).
- **Carga no MySQL:** Insere os registros em lotes otimizados.
- **Exportação:** Gera tabelas resumidas e exporta para novos arquivos CSV.
- **Validação:** Valida CNPJs, limpa telefones e trata e-mails.
- **Geração de Leads:** Separa os dados processados em segmentos como LTDA e MEI/ME.

---
## 📂 Estrutura de Pastas

```

📁 arquivo receita           # Onde os arquivos ZIP são salvos <br>
📁 arquivo receita deszipados # Onde os CSVs descompactados são armazenados<br>
📁 dividir                    # Pastas para armazenar CSVs divididos em partes<br>
📁 export                     # Exportações finais<br>
📁 LEADS                      # Leads prontos para uso<br>
📄 main.py                    # Código principal<br>

````

---

## 🚀 Como Executar

1. **Instale as dependências:**  
   Certifique-se de ter o Python e o MySQL instalados.
   ```bash
   pip install -r requirements.txt
``

2. **Configure o Banco:**
   Ajuste as credenciais do MySQL no início do `main.py`:

   ```python
   DBsenha = 'SUA_SENHA_AQUI'
   BDdataBase = 'SEU_BANCO_AQUI'
   ```

3. **Execute o script:**

   ```bash
   python main.py
   ```

   O processo completo:

   * Baixa os arquivos mais recentes
   * Descompacta os ZIPs
   * Divide os CSVs em partes menores
   * Cria/Recria as tabelas no banco
   * Insere os dados no banco
   * Exporta a tabela final
   * Gera os leads segmentados

---

## 👨‍💻 Agradecimentos

Este projeto foi desenvolvido por **\[Seu Nome]**, com ajuda e orientação de **Wilgner**, que ensinou conceitos de descriptografia, carga de dados e automação para Telecom.

---

## 📜 Licença

Este projeto é de uso **educacional e interno**. Verifique as permissões da base de dados da Receita Federal antes de redistribuir ou usar comercialmente.

---

## 📞 Contato

Para dúvidas ou sugestões:

* **Mateus e Wilgner**
* **Email:** [mateusmonteito57@gmail.com](mailto:mateusmonteito57@gmail.com)

---

🚀 **Feito para ajudar Telecoms a subir dados de forma ágil e confiável!**

```
