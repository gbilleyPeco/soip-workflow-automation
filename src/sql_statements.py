# =============================================================================
# This file contains SQL statements to pull data from PECO's data warehouse.
# =============================================================================

tbl_tab_Location_sql = 'select * from tbl_tab_Location;'

nbr_of_depots_sql = """
select a.movetype as movetype,
	a.Customer as customer,
	Count(a.Depot) as number_of_depots 
from 
	(
	select case 
				when Left(a.TNumber, 2) = 'IS' then 'Issue' 
				when Left(a.TNumber, 1) = 'R' then 'Return' 
				else '' 
			end as movetype,
		case 
				when Left(a.TNumber, 2) = 'IS' then b.Code 
				when Left(a.TNumber, 1) = 'R' then c.Code 
				else '' 
			end as Depot,
		case 
				when Left(a.TNumber, 2) = 'IS' then c.Code 
				when Left(a.TNumber, 1) = 'R' then b.Code 
				else '' 
			end as Customer,
		Sum(a.RL_T_Actual_Qty_From) as total_pallets 
	from mtms a 
		left join tbl_tab_Location b on a.RL_T_Location_From = b.IID 
		left join tbl_tab_Location c on a.RL_T_Location_To = c.IID 
	where DateDiff(day, Cast(a.RL_T_Date_To as date), Cast(GetDate() as date)) <= 90 
	group by case 
				when Left(a.TNumber, 2) = 'IS' then 'Issue' 
				when Left(a.TNumber, 1) = 'R' then 'Return' 
				else '' 
			end, case 
				when Left(a.TNumber, 2) = 'IS' then b.Code 
				when Left(a.TNumber, 1) = 'R' then c.Code 
				else '' 
			end, case 
				when Left(a.TNumber, 2) = 'IS' then c.Code 
				when Left(a.TNumber, 1) = 'R' then b.Code 
				else '' 
			end
	) a 
group by a.movetype, a.Customer
;
"""

# =============================================================================
# transport_rates_hist_load_counts_sql = """
# select a.*,
# 	case 
# 			when a.CPU_Loads > 0 and a.Dedicated_Loads > 0 then 'Split' 
# 			when a.CPU_Loads > 0 and a.Other_Loads > 0 then 'Split' 
# 			when a.Dedicated_Loads > 0 and a.Other_Loads > 0 then 'Split' 
# 			when a.Dedicated_Loads > 0 and a.Other_Loads > 0 and a.CPU_Loads > 0 then 'Split' 
# 			else '' 
# 		end as Split_Carrier_Type_Flag 
# from 
# 	(
# 	select case 
# 				when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
# 				when Left(mtms.TNumber, 1) = 'R' then 'Return' 
# 				when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
# 				else '' 
# 			end as movetype,
# 		case 
# 				when Left(mtms.TNumber, 2) = 'IS' then b.Code 
# 				when Left(mtms.TNumber, 1) = 'R' then c.Code 
# 				else '' 
# 			end as Depot,
# 		case 
# 				when Left(mtms.TNumber, 2) = 'IS' then c.Code 
# 				when Left(mtms.TNumber, 1) = 'R' then b.Code 
# 				else '' 
# 			end as Customer,
# 		b.Code + '-' + c.Code as Lane_ID,
# 		b.Name as origin_Name,
# 		c.Name as Destination_Name,
# 		Sum(
# 			case 
# 					when mtms.TMS_CarrierSCAC in ('CPU', 'KFPM', 'KRFN', 'IBCO', 'BOZZ', 'LOBW', 'MRDI', 'BGLT', 'FRTRN', 'SWCO') then 1 
# 					else 0 
# 				end) as CPU_Loads,
# 		Sum(
# 			case 
# 					when mtms.TMS_CarrierSCAC in ('AWAW', 'AWSL', 'AWTO', 'CLBC', 'CLPY', 'CPQP', 'GARP', 'GDKP', 'HJBD', 'HJCS', 'JBDD', 'JBTA', 'JDCS', 'JITX', 'PLMQ', 'PPIR', 'SYDW', 'UDLC', 'WPSL') then 1 
# 					else 0 
# 				end) as Dedicated_Loads,
# 		Sum(
# 			case 
# 					when mtms.TMS_CarrierSCAC not in ('JDCS', 'HJCS', 'SYDW', 'JBDD', 'GDKP', 'CLPY', 'HJBD', 'PLMQ', 'UDLC', 'AWTO', 'PPIR', 'JITX', 'DAAI', 'GARP', 'CPU', 'KFPM', 'KRFN', 'IBCO', 'BOZZ', 'LOBW', 'MRDI', 'BGLT', 'FRTRN', 'SWCO') then 1 
# 					else 0 
# 				end) as Other_Loads 
# 	from mtms mtms 
# 		left join tbl_tab_Location b on mtms.RL_T_Location_From = b.IID 
# 		left join tbl_tab_Location c on mtms.RL_T_Location_To = c.IID 
# 	where DateDiff(day, Cast(mtms.RL_T_Date_To as date), Cast(GetDate() as date)) <= 60 
# 		and mtms.RL_O_Status <> 5 
# 		and Left(mtms.TNumber, 3) <> 'DDT' 
# 		and mtms.TMS_CarrierSCAC is not NULL 
# 	group by case 
# 				when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
# 				when Left(mtms.TNumber, 1) = 'R' then 'Return' 
# 				when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
# 				else '' 
# 			end, case 
# 				when Left(mtms.TNumber, 2) = 'IS' then b.Code 
# 				when Left(mtms.TNumber, 1) = 'R' then c.Code 
# 				else '' 
# 			end, case 
# 				when Left(mtms.TNumber, 2) = 'IS' then c.Code 
# 				when Left(mtms.TNumber, 1) = 'R' then b.Code 
# 				else '' 
# 			end, b.Code + '-' + c.Code, b.Name, c.Name
# 	) as a
# """
# 
# transport_rates_hist_costs_sql = """
# select case 
# 			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
# 			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
# 			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
# 			else '' 
# 		end as movetype,
# 	case 
# 			when Left(mtms.TNumber, 2) = 'IS' then b.Code 
# 			when Left(mtms.TNumber, 1) = 'R' then c.Code 
# 			else '' 
# 		end as Depot,
# 	case 
# 			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
# 			when Left(mtms.TNumber, 1) = 'R' then b.Code 
# 			else '' 
# 		end as Customer,
# 	b.Code + '-' + c.Code as Lane_ID,
# 	mtms.TMS_CarrierSCAC as SCAC,
# 	case 
# 			when mtms.TMS_CarrierSCAC in ('CPU', 'KFPM', 'KRFN', 'IBCO', 'BOZZ', 'LOBW', 'MRDI', 'BGLT', 'FRTRN', 'SWCO') then 'CPU' 
# 			when mtms.TMS_CarrierSCAC in ('AWAW', 'AWSL', 'AWTO', 'CLBC', 'CLPY', 'CPQP', 'GARP', 'GDKP', 'HJBD', 'HJCS', 'JBDD', 'JBTA', 'JDCS', 'JITX', 'PLMQ', 'PPIR', 'SYDW', 'UDLC', 'WPSL') then 'Dedicated' 
# 			else 'Other' 
# 		end as Carrier_Type,
# 	Sum(
# 		case 
# 				when IsNull(mtms.TMS_InvoiceTotalLineHaul, 0) <> 0 then (IsNull(mtms.TMS_InvoiceTotalLineHaul, 0) + IsNull(mtms.TMS_InvoiceTotalFuel, 0) + IsNull(mtms.TMS_InvoiceTotalOther, 0) + IsNull(mtms.TMS_InvoiceTotalDetention, 0) + IsNull(mtms.TMS_InvoiceTotalTax, 0)) * ((mtms.TMS_CarrierNormCharge) / NullIf((mtms.TMS_CarrierCharge), 0)) 
# 				else IsNull(mtms.TMS_CarrierNormCharge, 0) 
# 			end) as Ttl_Cost,
# 	Sum(mtms.RL_T_Actual_Qty_From * -1) as Volume,
# 	Count(mtms.RL_T_Actual_Qty_From) as Total_Loads,
# 	Sum(IsNull(mtms.TMS_InvoiceTotalLineHaul, 0)) as Ttl_Fuel_Cost,
# 	Sum(IsNull(mtms.TMS_InvoiceTotalLineHaul, 0)) as Ttl_LH_Cost,
# 	c.Code as ToCode,
# 	c.[NAV Location Type] as ToType,
# 	b.Code as FromCode,
# 	b.[NAV Location Type] as FromType,
# 	mtms.RL_O_Creation_Date,
# 	mtms.RL_O_Delivery_Date,
# 	mtms.RL_PL_Date_To,
# 	mtms.RL_PL_Date_From,
# 	mtms.RL_T_Date_From,
# 	mtms.RL_T_Date_To,
# 	mtms.TMS_TransActualShip,
# 	mtms.TMS_ActualDelivery,
# 	mtms.TNumber 
# from mtms mtms 
# 	left join tbl_tab_Location b on mtms.RL_T_Location_From = b.IID 
# 	left join tbl_tab_Location c on mtms.RL_T_Location_To = c.IID 
# where DateDiff(month, Cast(mtms.RL_T_Date_To as date), Cast(GetDate() as date)) <= 3 
# 	and mtms.RL_O_Status <> 5 
# 	and Left(mtms.TNumber, 3) <> 'DDT' 
# 	and mtms.TMS_CarrierDistance is not NULL 
# group by case 
# 			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
# 			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
# 			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
# 			else '' 
# 		end, case 
# 			when Left(mtms.TNumber, 2) = 'IS' then b.Code 
# 			when Left(mtms.TNumber, 1) = 'R' then c.Code 
# 			else '' 
# 		end, case 
# 			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
# 			when Left(mtms.TNumber, 1) = 'R' then b.Code 
# 			else '' 
# 		end, b.Code + '-' + c.Code, mtms.TMS_CarrierSCAC, case 
# 			when mtms.TMS_CarrierSCAC in ('CPU', 'KFPM', 'KRFN', 'IBCO', 'BOZZ', 'LOBW', 'MRDI', 'BGLT', 'FRTRN', 'SWCO') then 'CPU' 
# 			when mtms.TMS_CarrierSCAC in ('AWAW', 'AWSL', 'AWTO', 'CLBC', 'CLPY', 'CPQP', 'GARP', 'GDKP', 'HJBD', 'HJCS', 'JBDD', 'JBTA', 'JDCS', 'JITX', 'PLMQ', 'PPIR', 'SYDW', 'UDLC', 'WPSL') then 'Dedicated' 
# 			else 'Other' 
# 		end, c.Code, c.[NAV Location Type], b.Code, b.[NAV Location Type], mtms.RL_O_Creation_Date, mtms.RL_O_Delivery_Date, mtms.RL_PL_Date_To, mtms.RL_PL_Date_From, mtms.RL_T_Date_From, mtms.RL_T_Date_To, mtms.TMS_TransActualShip, mtms.TMS_ActualDelivery, mtms.TNumber
# """
# =============================================================================

trans_load_size_sql = """
select case 
			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
			else '' 
		end as movetype,
	case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
			when Left(mtms.TNumber, 1) = 'R' then b.Code 
			else '' 
		end customer_Loc_Code,
	case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Name 
			when Left(mtms.TNumber, 1) = 'R' then b.Name 
			else '' 
		end customer_Loc_Name,
	Count(IsNull(mtms.RL_T_Actual_Qty_From, 0)) as Load_Count,
	Avg(IsNull(mtms.RL_T_Actual_Qty_To, 0)) as Average_Cube 
from mtms mtms 
	left join tbl_tab_Location b on mtms.RL_T_Location_From = b.IID 
	left join tbl_tab_Location c on mtms.RL_T_Location_To = c.IID 
where DateDiff(day, Cast(mtms.RL_T_Date_To as date), Cast(GetDate() as date)) <= 60 
	and mtms.RL_O_Status <> 5 
	and Left(mtms.TNumber, 3) <> 'DDT' 
	and Left(mtms.TNumber, 3) <> 'TOR' 
	and mtms.RL_T_Date_To is not NULL 
	and mtms.TMS_CarrierSCAC is not NULL 
group by case 
			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
			else '' 
		end, case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
			when Left(mtms.TNumber, 1) = 'R' then b.Code 
			else '' 
		end, case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Name 
			when Left(mtms.TNumber, 1) = 'R' then b.Name 
			else '' 
		end
"""

trans_load_counts_sql_raw = """
select a.*,
	case 
			when a.CPU_Loads > 0 and a.Dedicated_Loads > 0 then 'Split' 
			when a.CPU_Loads > 0 and a.Other_Loads > 0 then 'Split' 
			when a.Dedicated_Loads > 0 and a.Other_Loads > 0 then 'Split' 
			when a.Dedicated_Loads > 0 and a.Other_Loads > 0 and a.CPU_Loads > 0 then 'Split' 
			else '' 
		end as Split_Carrier_Type_Flag 
from 
	(
	select case 
				when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
				when Left(mtms.TNumber, 1) = 'R' then 'Return' 
				when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
				else '' 
			end as movetype,
		case 
				when Left(mtms.TNumber, 2) = 'IS' then b.Code 
				when Left(mtms.TNumber, 1) = 'R' then c.Code 
				else '' 
			end as Depot,
		case 
				when Left(mtms.TNumber, 2) = 'IS' then c.Code 
				when Left(mtms.TNumber, 1) = 'R' then b.Code 
				else '' 
			end as Customer,
		b.Code + '-' + c.Code as Lane_ID,
		b.Name as origin_Name,
		c.Name as Destination_Name,
		Sum(
			case 
					when mtms.TMS_CarrierSCAC in __CPU_SCACS__ then 1 
					else 0 
				end) as CPU_Loads,
		Sum(
			case 
					when mtms.TMS_CarrierSCAC in __DED_SCACS__ then 1 
					else 0 
				end) as Dedicated_Loads,
		Sum(
			case 
					when mtms.TMS_CarrierSCAC not in __CPU_DED_SCACS__ then 1 
					else 0 
				end) as Other_Loads 
	from mtms mtms 
		left join tbl_tab_Location b on mtms.RL_T_Location_From = b.IID 
		left join tbl_tab_Location c on mtms.RL_T_Location_To = c.IID 
	where DateDiff(day, Cast(mtms.RL_T_Date_To as date), Cast(GetDate() as date)) <= 60 
		and mtms.RL_O_Status <> 5 
		and Left(mtms.TNumber, 3) <> 'DDT' 
		and mtms.TMS_CarrierSCAC is not NULL 
	group by case 
				when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
				when Left(mtms.TNumber, 1) = 'R' then 'Return' 
				when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
				else '' 
			end, case 
				when Left(mtms.TNumber, 2) = 'IS' then b.Code 
				when Left(mtms.TNumber, 1) = 'R' then c.Code 
				else '' 
			end, case 
				when Left(mtms.TNumber, 2) = 'IS' then c.Code 
				when Left(mtms.TNumber, 1) = 'R' then b.Code 
				else '' 
			end, b.Code + '-' + c.Code, b.Name, c.Name
	) as a
"""

trans_costs_sql_raw = """
select case 
			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
			else '' 
		end as movetype,
	case 
			when Left(mtms.TNumber, 2) = 'IS' then b.Code 
			when Left(mtms.TNumber, 1) = 'R' then c.Code 
			else '' 
		end as Depot,
	case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
			when Left(mtms.TNumber, 1) = 'R' then b.Code 
			else '' 
		end as Customer,
	b.Code + '-' + c.Code as Lane_ID,
	mtms.TMS_CarrierSCAC as SCAC,
	case 
			when mtms.TMS_CarrierSCAC in __CPU_SCACS__ then 'CPU' 
			when mtms.TMS_CarrierSCAC in __DED_SCACS__ then 'Dedicated' 
			else 'Other' 
		end as Carrier_Type,
	Sum(
		case 
				when IsNull(mtms.TMS_InvoiceTotalLineHaul, 0) <> 0 then (IsNull(mtms.TMS_InvoiceTotalLineHaul, 0) + IsNull(mtms.TMS_InvoiceTotalFuel, 0) + IsNull(mtms.TMS_InvoiceTotalOther, 0) + IsNull(mtms.TMS_InvoiceTotalDetention, 0) + IsNull(mtms.TMS_InvoiceTotalTax, 0)) * ((mtms.TMS_CarrierNormCharge) / NullIf((mtms.TMS_CarrierCharge), 0)) 
				else IsNull(mtms.TMS_CarrierNormCharge, 0) 
			end) as Ttl_Cost,
	Sum(mtms.RL_T_Actual_Qty_From * -1) as Volume,
	Count(mtms.RL_T_Actual_Qty_From) as Total_Loads,
	Sum(IsNull(mtms.TMS_InvoiceTotalLineHaul, 0)) as Ttl_Fuel_Cost,
	Sum(IsNull(mtms.TMS_InvoiceTotalLineHaul, 0)) as Ttl_LH_Cost,
	c.Code as ToCode,
	c.[NAV Location Type] as ToType,
	b.Code as FromCode,
	b.[NAV Location Type] as FromType,
	mtms.RL_O_Creation_Date,
	mtms.RL_O_Delivery_Date,
	mtms.RL_PL_Date_To,
	mtms.RL_PL_Date_From,
	mtms.RL_T_Date_From,
	mtms.RL_T_Date_To,
	mtms.TMS_TransActualShip,
	mtms.TMS_ActualDelivery,
	mtms.TNumber 
from mtms mtms 
	left join tbl_tab_Location b on mtms.RL_T_Location_From = b.IID 
	left join tbl_tab_Location c on mtms.RL_T_Location_To = c.IID 
where DateDiff(month, Cast(mtms.RL_T_Date_To as date), Cast(GetDate() as date)) <= 3 
	and mtms.RL_O_Status <> 5 
	and Left(mtms.TNumber, 3) <> 'DDT' 
	and mtms.TMS_CarrierDistance is not NULL 
group by case 
			when Left(mtms.TNumber, 2) = 'IS' then 'Issue' 
			when Left(mtms.TNumber, 1) = 'R' then 'Return' 
			when Left(mtms.TNumber, 3) = 'TOR' then 'Transfer' 
			else '' 
		end, case 
			when Left(mtms.TNumber, 2) = 'IS' then b.Code 
			when Left(mtms.TNumber, 1) = 'R' then c.Code 
			else '' 
		end, case 
			when Left(mtms.TNumber, 2) = 'IS' then c.Code 
			when Left(mtms.TNumber, 1) = 'R' then b.Code 
			else '' 
		end, b.Code + '-' + c.Code, mtms.TMS_CarrierSCAC, case 
			when mtms.TMS_CarrierSCAC in ('CPU', 'KFPM', 'KRFN', 'IBCO', 'BOZZ', 'LOBW', 'MRDI', 'BGLT', 'FRTRN', 'SWCO') then 'CPU' 
			when mtms.TMS_CarrierSCAC in ('AWAW', 'AWSL', 'AWTO', 'CLBC', 'CLPY', 'CPQP', 'GARP', 'GDKP', 'HJBD', 'HJCS', 'JBDD', 'JBTA', 'JDCS', 'JITX', 'PLMQ', 'PPIR', 'SYDW', 'UDLC', 'WPSL') then 'Dedicated' 
			else 'Other' 
		end, c.Code, c.[NAV Location Type], b.Code, b.[NAV Location Type], mtms.RL_O_Creation_Date, mtms.RL_O_Delivery_Date, mtms.RL_PL_Date_To, mtms.RL_PL_Date_From, mtms.RL_T_Date_From, mtms.RL_T_Date_To, mtms.TMS_TransActualShip, mtms.TMS_ActualDelivery, mtms.TNumber
"""