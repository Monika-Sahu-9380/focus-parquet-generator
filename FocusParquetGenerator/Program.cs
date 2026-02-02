using Parquet;
using Parquet.Schema;
using Parquet.Data;

var rows = new List<Dictionary<string, object>>();

var startDate = new DateTime(2025, 1, 1);
var endDate = new DateTime(2026, 12, 31);

int id = 1;

for (var date = startDate; date <= endDate; date = date.AddDays(1))
{
    // Two services per day
    foreach (var service in new[] { "App Service", "Database" })
    {
        rows.Add(new Dictionary<string, object>
        {
            ["AvailabilityZone"] = "zone-1",
            ["BilledCost"] = service == "App Service" ? 25.0 + id % 5 : 40.0 + id % 7,
            ["BillingAccountId"] = (double?)1,
            ["BillingAccountName"] = "DemoAccount",
            ["BillingCurrency"] = "EUR",
            ["BillingPeriodStart"] = date.ToString("yyyy-MM-dd"),
            ["BillingPeriodEnd"] = date.AddDays(1).ToString("yyyy-MM-dd"),
            ["ChargeCategory"] = "Usage",
            ["ChargeClass"] = "Compute",
            ["ChargeDescription"] = $"{service} usage",
            ["ChargeFrequency"] = "Daily",
            ["ChargePeriodStart"] = date,
            ["ChargePeriodEnd"] = date.ToString("yyyy-MM-dd"),
            ["CommitmentDiscountCategory"] = "None",
            ["CommitmentDiscountId"] = "CD1",
            ["CommitmentDiscountName"] = "None",
            ["CommitmentDiscountStatus"] = "Inactive",
            ["CommitmentDiscountType"] = "None",
            ["ConsumedQuantity"] = service == "App Service" ? 10.0 : 20.0,
            ["ConsumedUnit"] = "Hours",
            ["ContractedCost"] = 100.0,
            ["ContractedUnitPrice"] = 10.0,
            ["EffectiveCost"] = 90.0,
            ["InvoiceIssuerName"] = "Azure",
            ["ListCost"] = 120.0,
            ["ListUnitPrice"] = 12.0,
            ["PricingCategory"] = "Standard",
            ["PricingQuantity"] = 1.0,
            ["PricingUnit"] = (double?)1,
            ["ProviderName"] = "Azure",
            ["PublisherName"] = "Microsoft",
            ["RegionId"] = "eu-west",
            ["RegionName"] = "Europe",
            ["ResourceId"] = $"res-{id}",
            ["ResourceName"] = service == "App Service" ? $"app-{id}" : $"db-{id}",
            ["ResourceType"] = service,
            ["ServiceCategory"] = "Compute",
            ["ServiceName"] = service,
            ["SkuId"] = $"sku-{id}",
            ["SkuPriceId"] = $"price-{id}",
            ["SubAccountId"] = "sub-1",
            ["SubAccountName"] = "Demo Sub",
            ["Tags"] = "{}",
            ["x_ServiceSubcategory"] = service == "App Service" ? "WebApp" : "SQL"
        });

        id++;
    }
}

List<DataField> fields = new();

foreach (var kv in rows.First())
{
    if (kv.Value is double)
        fields.Add(new DataField<double?>(kv.Key));
    else if (kv.Value is DateTime)
        fields.Add(new DataField<DateTime>(kv.Key));
    else
        fields.Add(new DataField<string>(kv.Key));
}

var schema = new ParquetSchema(fields);

using var stream = File.Create("demo.parquet");
using var writer = await ParquetWriter.CreateAsync(schema, stream);
using var rowGroup = writer.CreateRowGroup();

foreach (var field in schema.GetDataFields())
{
    if (field.ClrType == typeof(string))
    {
        var data = rows.Select(r => (string?)r[field.Name]).ToArray();
        await rowGroup.WriteColumnAsync(new DataColumn(field, data));
    }
    else if (field.ClrType == typeof(double) && field.IsNullable)
    {
        var data = rows.Select(r =>
            r[field.Name] == null ? (double?)null : Convert.ToDouble(r[field.Name])
        ).ToArray();

        await rowGroup.WriteColumnAsync(new DataColumn(field, data));
    }
    else if (field.ClrType == typeof(double) && !field.IsNullable)
    {
        var data = rows.Select(r => Convert.ToDouble(r[field.Name])).ToArray();
        await rowGroup.WriteColumnAsync(new DataColumn(field, data));
    }
    else if (field.ClrType == typeof(DateTime))
    {
        var data = rows.Select(r => (DateTime)r[field.Name]).ToArray();
        await rowGroup.WriteColumnAsync(new DataColumn(field, data));
    }
    else
    {
        throw new Exception($"Unsupported field: {field.Name} ({field.ClrType}, nullable={field.IsNullable})");
    }
}

Console.WriteLine($"Generated {rows.Count} rows into demo.parquet");