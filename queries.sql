
-- 1. Total Sales by Product Write a SQL query to calculate the total sales amount for each product, 
-- sorted by the total sales amount in descending order.

select 
    pd.productid, 
	pd.name, 
	sum(sf.quantity * sf.price) as total_sales 
from 
    public.product_dim pd 
left join 
    public.sales_fact sf 
    on pd.productid = sf.productid
group by 
    pd.productid
order by 
    total_sales desc;

-- 2. Sales by Month and Channel Write a SQL query to find the total sales amount and total quantity 
-- sold for each month and channel.
select
	dd.month,
	rd.channel,
	sum(sf.quantity) as total_quantity,
	sum(sf.price * sf.quantity) as total_sales
from public.sales_fact sf
join date_dim dd on dd.date = sf.date
join retailer_dim rd on rd.retailerid = sf.retailerid
group by
	dd.month,
	rd.channel
order by
	dd.month,
	rd.channel


-- 3. Top Selling Product by Category for Each Retailer Write a SQL query to identify 
-- the top selling product by total sales amount in each category for each retailer.
With CTE_1 as (
select
	 rd.retailerid,
	 pd.category, 
     pd.productid,
     pd.name,
	 sum(sf.quantity * sf.price) as total_sales,
     dense_rank()over(partition by pd.category, rd.retailerid order by sum(sf.quantity * sf.price) desc) as rnk

from 
    public.sales_fact sf
left join 
    retailer_dim rd 
    on sf.retailerid = rd.retailerid
left join
	public.product_dim pd 
    on sf.productid = pd.productid
group by 
	 rd.retailerid,
	 pd.category, 
     pd.productid,
     pd.name
)
select 
	 retailerid,
	 category, 
     productid,
     name,
	 total_sales
from 
    CTE_1
where 
    rnk=1
order by
    retailerid;