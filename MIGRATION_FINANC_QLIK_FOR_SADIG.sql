USE [BusinessCloud]
GO
/****** Object:  StoredProcedure [dbo].[MIGRATION_FINANC_QLIK_FOR_BusinessCloud]    Script Date: 27/01/2017 19:07:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE procedure [dbo].[MIGRATION_FINANC_QLIK_FOR_SADIG]

AS 

truncate table dbo.FatoContasReceber

BULK
INSERT dbo.FatoContasReceber 
FROM '\\TIFON\Transformacao\FatoContasReceber.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252

)

truncate table dbo.FatoContasPagar

BULK
INSERT dbo.FatoContasPagar 
FROM '\\TIFON\Transformacao\FatoContasPagar.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252
)

truncate table dbo.Emitente 

BULK
INSERT dbo.Emitente 
FROM '\\TIFON\Transformacao\Emitente.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252
)

truncate table dbo.Representante

BULK
INSERT dbo.Representante 
FROM '\\TIFON\Transformacao\Representante.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252 
)

truncate table dbo.Empresa 

BULK
INSERT dbo.Empresa 
FROM '\\TIFON\Transformacao\Empresa.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252
)


truncate table dbo.Estabelecimento 

BULK
INSERT dbo.Estabelecimento 
FROM '\\TIFON\Transformacao\Estabelecimento.csv' 
WITH
(
FIELDTERMINATOR = '|',
ROWTERMINATOR = '\n',
FIRSTROW=2,
CODEPAGE = 1252
)

	if object_id('tempdb.dbo.##tempmes') is not null  drop table tempdb.dbo.##tempmes


	select 1 as idmes,'01-JAN' as nomes into ##tempmes
	union all
	select 2 as idmes,'02-FEV' as nomes
	union all
	select 3 as idmes,'03-MAR' as nomes 
	union all
	select 4 as idmes,'04-ABR' as nomes 
	union all
	select 5 as idmes,'05-MAI' as nomes 
	union all
	select 6 as idmes,'06-JUN' as nomes 
	union all
	select 7 as idmes,'07-JUL' as nomes 
	union all
	select 8 as idmes,'08-AGO' as nomes 
	union all
	select 9 as idmes,'09-SET' as nomes 
	union all
	select 10 as idmes,'10-OUT' as nomes 
	union all
	select 11 as idmes,'11-NOV' as nomes 
	union all
	select 12 as idmes,'12-DEZ' as nomes 

/* Importa Contas a Receber */

if object_id('BusinessCloud.dbo.StageContasReceber') is not null  drop table BusinessCloud.dbo.StageContasReceber 

SELECT
       [NumeroTitulo]
	  --,[cdn_clien_matriz]
      ,[NumeroTituloBanco]
	  ,convert(datetime,[DataMovimento],103) as [DataMovimento]
	  ,convert(varchar,[DataMovimento],103) as [DiaMovimento]
	  ,mesmov.nomes as [MesMovimento]
	  ,cast(year(convert(date,[DataMovimento],103)) as varchar(10)) as [AnoMovimento]	
      ,Est.Estabelecimento
	  ,Emi.Cliente
	  ,Emi.EstadoCliente
	  ,Emi.CidadeCliente
	  ,Emi.Regiao
	  ,Emi.natureza
      ,Emp.NomeEmpresa
      ,[SistuacaoTItulo]
	  ,[TipoEspecieDocto]
      ,[CarteiraBancaria]
      ,[OrigemTituloACR]
	  ,[COD_ESPEC_DOCTO]
      ,[ParcelaTitulo]
      ,NomeAbrev as Representante
      ,[CODPORTADOR]
      ,[CarteriaBancariaDfescricao]
	  ,cast(
	  isnull(replace(
	  case when [AdiantamentoACliente] = '' then '0' else [AdiantamentoACliente] end ,',','.'),0) 
	  as dec(15,2)) as [AdiantamentoACliente]
	  ,Convert(datetime,[DataEmissaoTitulo],103) as [DataEmissaoTitulo]
	  ,Convert(varchar,[DataEmissaoTitulo],103) as [DiaEmissaoTitulo]
	  ,mesemi.nomes as [MesEmissaoTitulo]
	  ,cast(Year(Convert(date,[DataEmissaoTitulo],103)) as varchar(10)) as [AnoEmissaoTitulo]

    
	  ,Convert(datetime,[DataTransacao],103) as [DataTransacao]
	  ,Convert(varchar,[DataTransacao],103) as [DiaTransacao]
	  ,mestrn.nomes as [MesTransacao]
	  ,cast(Year(Convert(date,[DataTransacao],103)) as varchar(10)) as [AnoTransacao]

      ,convert(datetime,[DataLiquidacao],103) as [DataLiquidacao]
	  ,convert(varchar,[DataLiquidacao],103) as [DiaLiquidacao]
	  ,mesliq.nomes as [MesLiquidacao]
	  ,cast(Year(convert(date,[DataLiquidacao],103)) as varchar(10)) as [AnoLiquidacao]

	  ,convert(datetime,[dat_vencto_tit_acr],103) as [dat_vencto_tit_acr]
	  ,convert(varchar,[dat_vencto_tit_acr],103) as [dia_vencto_tit_acr]
	  ,mesven.nomes as [mes_vencto_tit_acr]
	  ,cast(year(convert(date,[dat_vencto_tit_acr],103)) as varchar(10)) as [ano_vencto_tit_acr]

	  -- Valores totalizadores

	 /* --,[ValorMovimentoFluxo]
	  ,cast(
	  replace(
	  case when [ValorMovimentoFluxo] = '' then '0' else [ValorMovimentoFluxo] end ,',','.') 
	  as dec(15,2)) as [ValorMovimentoFluxo] */

      --,[val_perc_cop_fluxo_cx]
   --   ,cast(
	  --isnull(replace(
	  --case when [val_perc_cop_fluxo_cx] = '' then '0' else [val_perc_cop_fluxo_cx] end ,',','.'),0) 
	  --as dec(15,2)) as [val_perc_cop_fluxo_cx]

	 -- ,[ValtitiAcrOrigMovtiacr]
	 -- ,cast(
	 --isnull( replace(
	 -- case when [ValtitiAcrOrigMovtiacr] = '' then '0' else [ValtitiAcrOrigMovtiacr] end ,',','.') ,0)
	 -- as int) as [ValtitiAcrOrigMovtiacr]

      
      --,[val_orig_movto_fluxo_cx]
	  --,cast(
	  --isnull(replace(
	  --case when [val_orig_movto_fluxo_cx] = '' then '0' else [val_orig_movto_fluxo_cx] end ,',','.'),0)
	  --as dec(15,2)) as [val_orig_movto_fluxo_cx]

      --,[val_movto_fluxo_cx_conver]
	  --,cast(
	  --isnull(replace(
	  --case when [val_movto_fluxo_cx_conver] = '' then '0' else [val_movto_fluxo_cx_conver] end ,',','.'),0) 
	  --as dec(15,2)) as [val_movto_fluxo_cx_conver]

	 -- ,[VrPago]
	  ,cast(
	  isnull(replace(
	  case when [VrPago] = '' then '0' else [VrPago] end ,',','.'),0)
	  as dec(15,2)) as [VrPago]

     -- ,[val_sdo_tit_acr]
	 -- ,cast(
	 --isnull(replace(
	 -- case when [val_sdo_tit_acr] = '' then '0' else [val_sdo_tit_acr] end ,',','.'),0) 
	 -- as dec(15,2)) as [val_sdo_tit_acr]

      --,[VrAbertoTitulo]
	  ,cast(
	  isnull(replace(
	  case when [VrAbertoTitulo] = '' then '0' else [VrAbertoTitulo] end ,',','.'),0) 
	  as dec(15,2)) as [VrAbertoTitulo]
	

      --,[VrVenc10dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc10dias] = '' then '0' else [VrVenc10dias] end ,',','.'),0)
	  as dec(15,2)) as [VrVenc10dias]

      --,[VrVenc30dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc30dias] = '' then '0' else [VrVenc30dias] end ,',','.'),0)
	  as dec(15,2)) as [VrVenc30dias]

      --,[VrVenc3160dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc3160dias] = '' then '0' else [VrVenc3160dias] end ,',','.'),0) 
	  as dec(15,2)) as [VrVenc3160dias]

      --,[VrVenc6190dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc6190dias] = '' then '0' else [VrVenc6190dias] end ,',','.'),0)
	  as dec(15,2)) as [VrVenc6190dias]

    --  ,[VrVenc91120dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc91120dias] = '' then '0' else [VrVenc91120dias] end ,',','.'),0)
	  as dec(15,2)) as [VrVenc91120dias]

     -- ,[VrVenc120dias]
	  ,cast(
	  isnull(replace(
	  case when [VrVenc120dias] = '' then '0' else [VrVenc120dias] end ,',','.'),0) 
	  as dec(15,2)) as [VrVenc120dias]

	  --[i_dias_receb]
      ,cast(
	  isnull(replace(
	  case when [i_dias_receb] = '' then '0' else [i_dias_receb] end ,',','.'),0) 
	  as int) as [i_dias_receb]

      --,[de_rec_ate_30d]
	  ,cast(
	  isnull(replace(
	  case when [de_rec_ate_30d] = '' then '0' else [de_rec_ate_30d] end ,',','.'),0) 
	  as dec(15,2)) as [de_rec_ate_30d]

      --,[de_rec_ate_31_60]
	   ,cast(
	  replace(
	  case when [de_rec_ate_31_60] = '' then '0' else [de_rec_ate_31_60] end ,',','.') 
	  as dec(15,2)) as [de_rec_ate_31_60]

      --,[de_rec_ate_61_90]
	  
	  ,cast(
	  isnull(replace(
	  case when [de_rec_ate_61_90] = '' then '0' else [de_rec_ate_61_90] end ,',','.'),0) 
	  as dec(15,2)) as [de_rec_ate_61_90]

      --,[de_rec_ate_91_120]
	  ,cast(
	  isnull(replace(
	  case when [de_rec_ate_91_120] = '' then '0' else [de_rec_ate_91_120] end ,',','.'),0) 
	  as dec(15,2)) as [de_rec_ate_91_120]

      --,[de_rec_acima_120]
	  ,cast(
	  isnull(replace(
	  case when [de_rec_acima_120] = '' then '0' else [de_rec_acima_120] end ,',','.'),0)
	  as dec(15,2)) as [de_rec_acima_120]

     -- ,[DiasAtrasoLiquidacao]
	  ,cast(
	  isnull(replace(
	  case when [DiasAtrasoLiquidacao] = '' then '0' else [DiasAtrasoLiquidacao] end ,',','.'),0) 
	  as int) as [DiasAtrasoLiquidacao]

     -- ,[DiasAtraso]
	  ,cast(
	  isnull(replace(
	  case when [DiasAtraso] = '' then '0' else [DiasAtraso] end ,',','.'),0)
	  as int) as [DiasAtraso]

	  --[VrDesconto]
	  ,cast(
	  isnull(replace(
	  case when [VrDesconto] = '' then '0' else [VrDesconto] end ,',','.'),0) 
	  as dec(15,2)) as [VrDesconto]

     --,[DiasVencimentoOrig]
	  
	  ,cast(
	  isnull(replace(
	  case when [DiasVencimentoOrig] = '' then '0' else [DiasVencimentoOrig] end ,',','.'),0) 
	  as int) as [DiasVencimentoOrig]

      --,[DiasVencimentoAlter]
	  ,cast(
	  isnull(replace(
	  case when [DiasVencimentoAlter] = '' then '0' else [DiasVencimentoAlter] end ,',','.'),0) 
	  as int) as [DiasVencimentoAlter]
     
	  --,[VrTotalaVencer]
	  ,cast(
	  isnull(replace(
	  case when [VrTotalaVencer] = '' then '0' else [VrTotalaVencer] end ,',','.'),0)
	  as dec(15,2)) as [VrTotalaVencer]

      ,[CategoriaAReceber]
      
	  --,[VrAReceber]
	  ,cast(
	  isnull(replace(
	  case when [VrAReceber] = '' then '0' else [VrAReceber] end ,',','.'),0) 
	  as dec(15,2)) as [VrAReceber]

      --,[VrTotalVencidoPago]
	  ,cast(
	  isnull(replace(
	  case when [VrTotalVencidoPago] = '' then '0' else [VrTotalVencidoPago] end ,',','.'),0) 
	  as dec(15,2)) as [VrTotalVencidoPago]

      --,[VLrVencidosAberto]
	  ,cast(
	  isnull(replace(
	  case when [VLrVencidosAberto] = '' then '0' else [VLrVencidosAberto] end ,',','.'),0) 
	  as dec(15,2)) as [VLrVencidosAberto]
      
	  --,[VlrInadimplencia]
	  ,cast(
	  isnull(replace(
	  case when [VlrInadimplencia] = '' then '0' else [VlrInadimplencia] end ,',','.'),0) 
	  as dec(15,2)) as [VlrInadimplencia]

     /* --,[VLRAtraso]
	  ,cast(
	  replace(
	  case when [VLRAtraso] = '' then '0' else [VLRAtraso] end ,',','.') 
	  as dec(15,2)) as [VLRAtraso]

      --,[VrTotalVencido]
	  ,cast(
	  replace(
	  case when [VrTotalVencido] = '' then '0' else [VrTotalVencido] end ,',','.') 
	  as dec(15,2)) as [VrTotalVencido]

      --,[Descontos]
	  ,cast(
	  replace(
	  case when [Descontos] = '' then '0' else [Descontos] end ,',','.') 
	  as dec(15,2)) as [Descontos]

      --  ,[val_perc_juros_dia_atraso]
	  ,cast(
	  replace(
	  case when [val_perc_juros_dia_atraso] = '' then '0' else [val_perc_juros_dia_atraso] end ,',','.') 
	  as dec(15,2)) as [val_perc_juros_dia_atraso]

   --   ,[val_perc_multa_atraso]
	  ,cast(
	  replace(
	  case when [val_perc_multa_atraso] = '' then '0' else [val_perc_multa_atraso] end ,',','.') 
	  as dec(15,2)) as [val_perc_multa_atraso]

     -- ,[val_perc_desc]
	  ,cast(
	  replace(
	  case when [val_perc_desc] = '' then '0' else [val_perc_desc] end ,',','.') 
	  as dec(15,2)) as [val_perc_desc]

     -- ,[valorOriginal]
	  ,cast(
	  replace(
	  case when [valorOriginal] = '' then '0' else [valorOriginal] end ,',','.') 
	  as dec(15,2)) as [valorOriginal]


      --,[ValorLiquidacao]
	  ,cast(
	  replace(
	  case when [ValorLiquidacao] = '' then '0' else [ValorLiquidacao] end ,',','.') 
	  as dec(15,2)) as [ValorLiquidacao]

      --,[val_desc_tit_acr]
	  ,cast(
	  replace(
	  case when [val_desc_tit_acr] = '' then '0' else [val_desc_tit_acr] end ,',','.') 
	  as dec(15,2)) as [val_desc_tit_acr]

      --,[valorSaldo]
	  ,cast(
	  replace(
	  case when [valorSaldo] = '' then '0' else [valorSaldo] end ,',','.') 
	  as dec(15,2)) as [valorSaldo]

      --,[val_cr_pis]
	  ,cast(
	  replace(
	  case when [val_cr_pis] = '' then '0' else [val_cr_pis] end ,',','.') 
	  as dec(15,2)) as [val_cr_pis]
      --,[val_cr_cofins]
	  ,cast(
	  replace(
	  case when [val_cr_cofins] = '' then '0' else [val_cr_cofins] end ,',','.') 
	  as dec(15,2)) as [val_cr_cofins]
      --,[val_cr_csll]
	   ,cast(
	  replace(
	  case when [val_cr_csll] = '' then '0' else [val_cr_csll] end ,',','.') 
	  as dec(15,2)) as [val_cr_csll] */

into StageContasReceber

  FROM [dbo].[FatoContasReceber] as Fcr with (nolock)

  left join [dbo].[Estabelecimento] as Est with (nolock) on Fcr.[COdEstabelacimento] = Est.COdEstabelacimento
  
  left join [dbo].[Empresa] as Emp with (nolock) on Fcr.COD_EMPRESA =  Emp.COD_EMPRESA
  
  left join [dbo].[Emitente] as Emi with (nolock) on Fcr.CODEMITENTE = Emi.CODEMITENTE

  left join [dbo].[Representante] as Rep with (nolock) on Fcr.CODREPRESENTANTE = Rep.CODREPRESENTANTE
 /* mapping meses - dimensão tempo */
left join ##tempmes mesmov on mesmov.idmes = month(convert(date,[DataMovimento],103))
left join ##tempmes mesemi on mesemi.idmes = month(convert(date,[DataEmissaoTitulo],103))
left join ##tempmes mesven on mesven.idmes = month(convert(date,[dat_vencto_tit_acr],103))
left join ##tempmes mestrn on mestrn.idmes = month(convert(date,[DataTransacao],103))
left join ##tempmes mesliq on mesliq.idmes = Month(convert(date,[DataLiquidacao],103))

where 

year(convert(date,[DataMovimento],103)) >=  year(dateadd(year,-2,getdate()))


/* Importa Contas Pagar */

if object_id('BusinessCloud.dbo.StageContasPagar') is not null  drop table BusinessCloud.dbo.StageContasPagar

SELECT 
	  [NumeroTitulo]
	  ,[SituacaoTitulo]
	  ,[HistoticoMovimento]
      --,[Secao]
      --,[cod_estab_orig]
	  --,[cod_unid_negoc]
	  ,Est.nome as NomeEstabelecimento
      ,Emt.Cliente as Fornecedor
      --,[CODPORTADOR] -- 
	  ,case when [DataLiquidaCaoTitulo] = '31/12/9999' or [DataLiquidaCaoTitulo] = '01/01/0001' then null else convert(datetime,[DataLiquidaCaoTitulo],103) end as [DataLiquidaCaoTitulo]
	  ,case when [DataLiquidaCaoTitulo] = '31/12/9999' or [DataLiquidaCaoTitulo] = '01/01/0001' then null else convert(varchar,[DataLiquidaCaoTitulo],103) end as  [DiaLiquidaCaoTitulo]
	  ,case when [DataLiquidaCaoTitulo] = '31/12/9999' or [DataLiquidaCaoTitulo] = '01/01/0001' then null else mesliq.nomes end as  [MEsLiquidaCaoTitulo]
	  ,case when [DataLiquidaCaoTitulo] = '31/12/9999' or [DataLiquidaCaoTitulo] = '01/01/0001' then null else cast(year(convert(date,[DataLiquidaCaoTitulo],103)) as varchar(10)) end  as  [AnoLiquidaCaoTitulo]
	  
	  ,convert(datetime,[DataMovimento],103) as [DataMovimento]
	  ,convert(varchar,[DataMovimento],103) as [DiaMovimento]
	  ,mesmov.nomes as [MesMovimento]
	  ,cast(year(convert(date,[DataMovimento],103)) as varchar(10)) as [AnoMovimento]

	  ,case when [DataEmissaoTitulo] = '10/08/0216' then '10/08/2016' else convert(datetime,[DataEmissaoTitulo],103) end as [DataEmissaoTitulo]
	  ,case when [DataEmissaoTitulo] = '10/08/0216' then '10/08/2016' else convert(varchar,[DataEmissaoTitulo],103) end as [DiaEmissaoTitulo]
	  ,mesemi.nomes as [MesEmissaoTitulo]
	  ,case when [DataEmissaoTitulo] = '10/08/0216' then '2016' else cast(year(convert(date,[DataEmissaoTitulo],103)) as varchar(10)) end as [AnoEmissaoTitulo]

	  ,convert(datetime,[DataVencimento],103) as [DataVencimento]
	  ,convert(varchar,[DataVencimento],103) as [DiaVencimento]
	  ,mesven.nomes as [MesVencimento]
	  ,cast(year(convert(date,[DataVencimento],103)) as varchar(10)) as [AnoVencimento]

	  ,convert(datetime,[DataPrevisaoPagamento],103) as [DataPrevisaoPagamento]
	  ,convert(varchar,[DataPrevisaoPagamento],103) as [DiaPrevisaoPagamento]
	  ,mesppg.nomes as [MesPrevisaoPagamento]
	  ,cast(year(convert(date,[DataPrevisaoPagamento],103)) as varchar(10)) as [AnoPrevisaoPagamento]

	  ,convert(datetime,[DataTransacao],103) as [DataTransacao]
	  ,convert(varchar,[DataTransacao],103) as [DiaTransacao]
	  ,mestrn.nomes as [MesTransacao]
	  ,cast(year(convert(date,[DataTransacao],103)) as varchar(10)) as [AnoTransacao]

	  ,convert(datetime,[DataUltimoPagamento],103) as [DataUltimoPagamento]
	  ,convert(varchar,[DataUltimoPagamento],103) as [DiaaUltimoPagamento]
	  ,mesupg.nomes as [MesUltimoPagamento]
	  ,cast(year(convert(date,[DataUltimoPagamento],103)) as varchar(10)) as [AnoUltimoPagamento]

	  ,cast(
	  isnull(replace(
	  case when [ValorMovimentoFluxo] = '' then '0' else [ValorMovimentoFluxo] end ,',','.'),0) 
	  as dec(15,2)) as [ValorMovimentoFluxo]

	  --,cast(
	  --replace(
	  --case when [val_perc_cop_fluxo_cx] = '' then '0' else [val_perc_cop_fluxo_cx] end ,',','.') 
	  --as dec(15,2)) as [val_perc_cop_fluxo_cx]

	  --,cast(
	  --isnull(replace(
	  --case when [ValtitiAcrOrigMovtitApb] = '' then '0' else [ValtitiAcrOrigMovtitApb] end ,',','.'),0)
	  --as dec(15,2)) as [ValtitiAcrOrigMovtitApb]

      --,[val_orig_movto_fluxo_cx]
	  --,cast(
	  --isnull(replace(
	  --case when [val_orig_movto_fluxo_cx] = '' then '0' else [val_orig_movto_fluxo_cx] end ,',','.'),0) 
	  --as dec(15,2)) as [val_orig_movto_fluxo_cx]

	  --,cast(
	  --isnull(replace(
	  --case when [val_movto_fluxo_cx_conver] = '' then '0' else [val_movto_fluxo_cx_conver] end ,',','.'),0) 
	  --as dec(15,2)) as [val_movto_fluxo_cx_conver]

      --,[val_sdo_tit_ap]
	  --,cast(
	  --isnull(replace(
	  --case when [val_sdo_tit_ap] = '' then '0' else [val_sdo_tit_ap] end ,',','.'),0) 
	  --as dec(15,2)) as [val_sdo_tit_ap]

   
	  ,cast(
	  isnull(replace(
	  case when [VrTotalaVencer] = '' then '0' else [VrTotalaVencer] end ,',','.'),0)
	  as dec(15,2)) as [VrTotalaVencer]
	  
	  ,cast(
	  isnull(replace(
	  case when [VrTotalVencido] = '' then '0' else [VrTotalVencido] end ,',','.'),0) 
	  as dec(15,2)) as [VrTotalVencido]
	  ,cast(
	  isnull(replace(
	  case when [Ap_APagar] = '' then '0' else [Ap_APagar] end ,',','.'),0) 
	  as dec(15,2)) as [Ap_APagar]
	  ,cast(
	  isnull(replace(
	  case when [VrOriginalTitulo] = '' then '0' else [VrOriginalTitulo] end ,',','.'),0) 
	  as dec(15,2)) as [VrOriginalTitulo]

	  ,cast(
	  isnull(replace(
	  case when [VrSaldoTitulo] = '' then '0' else [VrSaldoTitulo] end ,',','.'),0) 
	  as dec(15,2)) as [VrSaldoTitulo]
	  ,cast(
	  isnull(replace(
	  case when [VrDescontoTitulo] = '' then '0' else [VrDescontoTitulo] end ,',','.'),0) 
	  as dec(15,2)) as [VrDescontoTitulo]
	  ,cast(
	  isnull(replace(
	  case when [VrJurosTitulo] = '' then '0' else [VrJurosTitulo] end ,',','.'),0)
	  as dec(15,2)) as [VrJurosTitulo]
	  ,cast(
	  isnull(replace(
	  case when [VrAjuste]= '' then '0' else [VrAjuste] end ,',','.'),0) 
	  as dec(15,2)) as [VrAjuste]

	  ,cast(
	  isnull(replace(
	  case when [ValorPago] = '' then '0' else [ValorPago] end ,',','.'),0) 
	  as dec(15,2)) as [ValorPago]
	 
	  ,cast(
	  isnull(replace(
	  case when [AdiantamentoAforncedor] = '' then '0' else [AdiantamentoAforncedor] end ,',','.'),0) 
	  as dec(15,2)) as [AdiantamentoAforncedor]

	  ,cast(
	  isnull(replace(
	  case when [Ap_DiasAtraso] = '' then '0' else [Ap_DiasAtraso] end ,',','.'),0)
	  as int) as [Ap_DiasAtraso]

into StageContasPagar

FROM [dbo].[FatoContasPagar] Fcp 

left join [dbo].[Empresa] Emp with(nolock) on Fcp.COD_EMPRESA = Emp.COD_EMPRESA

left join dbo.Estabelecimento Est with(nolock) on Est.COdEstabelacimento = Fcp.COdEstabelacimento

/* mapping meses - dimensão tempo */
left join [dbo].[Emitente] Emt with(nolock) on Emt.CODEMITENTE = Fcp.CODEMITENTE
left join ##tempmes mesliq on mesliq.idmes = month(convert(date,[DataLiquidaCaoTitulo],103))
left join ##tempmes mesmov on mesmov.idmes = month(convert(date,[DataMovimento],103))
left join ##tempmes mesemi on mesemi.idmes = month(convert(date,[DataEmissaoTitulo],103))
left join ##tempmes mesven on mesven.idmes = month(convert(date,[DataVencimento],103))
left join ##tempmes mesppg on mesppg.idmes = month(convert(date,[DataPrevisaoPagamento],103))
left join ##tempmes mestrn on mestrn.idmes = month(convert(date,[DataTransacao],103))
left join ##tempmes mesupg on mesupg.idmes = month(convert(date,[DataUltimoPagamento],103))
