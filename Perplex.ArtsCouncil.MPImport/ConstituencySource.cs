using Perplex.Integration.Core;
using Perplex.Integration.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.ArtsCouncil.MPImport
{
	[Step]
    public class ConstituencySource : DataSourceStep
    {

        // "http://data.parliament.uk/membersdataplatform/open/OData.svc"
        [ConnectionString("OdataUrl", Type = "Odata")]
        public string OdataServerUrl { get; set; }

        public override void Execute()
        {
			var ctx = new MNISModel.MNISEntities(new Uri("http://data.parliament.uk/membersdataplatform/open/OData.svc"));

			var constituencies = ctx.Constituencies
				.Expand("ConstituencyAreas/Area")
				.Where(c => c.EndDate == null);
			foreach (var c in constituencies)
			{
				const int Country = 1;
				const int Region = 8;
				const int DistrictNI = 3;
				const int FormerMetCounty = 4;
				const int ShireCountyEngland = 9;
				const int UnitaryAuthorityScotland = 12;
				const int UnitaryAuthorityWales = 13;
				var areas = c.ConstituencyAreas.Where(ca => ca.EndDate == null && ca.Area != null).
					Select(ca => new { ca.Area.Name, Type = ca.Area.AreaType_Id }).Distinct().ToList();
				var row = new Row
				{
					["Id"] = c.Constituency_Id,
					["Name"] = c.Name,
					["Country"] = areas.FirstOrDefault(a => a.Type == Country).Name,
					["Region"] = areas.FirstOrDefault(a => a.Type == Region).Name,
					["Counties"] = areas.Where(a => a.Type == DistrictNI || a.Type == FormerMetCounty ||
						a.Type == ShireCountyEngland || a.Type == UnitaryAuthorityScotland ||
						a.Type == UnitaryAuthorityWales).Select(a => a.Name).ToArray()
				};
				Output.AddRow(row);
			}
		}
    }
}
