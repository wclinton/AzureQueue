using System;
using System.Globalization;
using Nephos.Model;
using Nephos.Model.Base;

namespace Utility
{
    public static class InvoiceUtil
    {
        public static Invoice CreateInvoice(int index)
        {
             DateTime FixedDate = DateTime.Now;
            var invoice = new Invoice
            {
                ExternalId = string.Format("{0}{1}", index, Environment.TickCount),
                ExternalReference = string.Format("{0}{1}", index, Environment.TickCount),
                EntityStatus = EntityStatus.Active,
                SyncEndpointTick = index,
                Type = InvoiceType.Other,
                Status = InvoiceStatus.Invoice,
                InvoiceNumber = index.ToString(CultureInfo.InvariantCulture),
                InvoiceDate = FixedDate,
                OrderNumber = index.ToString(CultureInfo.InvariantCulture),
                OrderDate = FixedDate,
                ShipDate = FixedDate,
                ShipVia = "UPS",
                Terms = "Terms code",
                Comments = "Testing",
                SubTotal = 100.00M,
                Discount = 10.00M,
                Freight = 10.00M,
                Taxes = 5.00M,
                PaymentDiscount = 2.00M,
                Balance = 140.00M,
                BillToFirstName = "Russell",
                BillToLastName = "Libby",
            };

            for (var i = 0; i < 10; i++)
            {
                var detail = new InvoiceDetail
                {
                    Invoice = invoice,
                    ExternalId = string.Format("{0}ABF{1}", index, i),
                    ExternalReference = string.Format("{0}ABF{1}", index, i),
                    LineItemType = LineItemType.ServiceType,
                    ItemNumber = "testing",
                    ItemDescription = string.Format("Testing {0}", i),
                    UnitOfMeasure = "EACH",
                    Quantity = 1,
                    QuantityShipped = 1,
                    QuantityBackOrdered = 0,
                    Warehouse = "001",
                    Price = 100.10M,
                    Total = 100.10M
                };
                invoice.InvoiceDetails.Add(detail);
            }

            return invoice;
        }
    }
}