using Perplex.Integration.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.ArtsCouncil.MPImport
{
    [Integration.Core.Configuration.Step("Parliament Members Source")]
    public class MPDownload : DataSourceStep
    {
        // "http://data.parliament.uk/membersdataplatform/open/OData.svc"
        [Integration.Core.Configuration.ConnectionString("OdataUrl", Type = "Odata")]
        public string OdataServerUrl { get; set; }

        public override void Execute()
        {
            Output.AddRows(GetAllMembers());
        }

        public IEnumerable<Member> GetAllMembers()
        {
            var ctx = new MNISModel.MNISEntities(new Uri(OdataServerUrl));
            var members = ctx.Members
                //.Expand("MemberParties/Party")
                //.Expand("MembershipFrom")
                .Expand("MemberPreferredNames")
                .Expand("MemberGovernmentPosts/GovernmentPost")
                .Expand("MemberOppositionPosts/OppositionPost")
                .Expand("MemberParliamentaryPosts/ParliamentaryPost")
                .Expand("MemberAddresses")
                .Where(m => m.CurrentStatusActive || m.EndDate > new DateTime(2015, 05, 06));

            foreach (var m in members)
            {
                var member = new Member()
                {
                    Id = m.Member_Id,
                    AddressAs = m.AddressAs ?? m.NameDisplayAs,
                    Party = m.Party,
                    IsActiveMember = m.CurrentStatusActive,
                    StartDate = m.StartDate,
                    EndDate = m.EndDate,
                    EndReason = m.EndReason,
                    DateOfBirth = m.DateOfBirth,
                    DateOfDeath = m.DateOfDeath
                };
                member.House = m.House;
                if (m.House == "Commons")
                {
                    member.Constituency = m.MembershipFrom;
                }
                else if (m.House == "Lords")
                {
                    member.TypeOfPeer = m.MembershipFrom;
                }
                // preferred name
                var preferredName = m.MemberPreferredNames
                    .Where(n => n.EndDate == null)
                    .OrderByDescending(n => n.StartDate)
                    .FirstOrDefault();
                if (preferredName != null)
                {
                    member.FirstName = preferredName.Forename;
                    member.MiddleNames = preferredName.MiddleNames;
                    member.LastName = preferredName.Surname;
                }
                else
                {
                    member.FirstName = m.Forename;
                    member.MiddleNames = m.MiddleNames;
                    member.LastName = m.Surname;
                }
                // email adresses
                var emailAddresses = m.MemberAddresses
                    .Where(a => !string.IsNullOrEmpty(a.Email) && (a.Email.Trim() != "contactholmember@parliament.uk"))
                    .OrderByDescending(a => a.IsPreferred)
                    .Select(a => a.Email.Trim())
                    .Distinct()
                    .ToList();
                if (emailAddresses.Count > 0) member.EmailAddress1 = emailAddresses[0];
                if (emailAddresses.Count > 1) member.EmailAddress2 = emailAddresses[1];
                if (emailAddresses.Count > 2) member.EmailAddress3 = emailAddresses[2];
                // Posts
                var parliamentaryPosts = m.MemberParliamentaryPosts
                        .Where(p => p.EndDate == null)
                        .Select(p => p.ParliamentaryPost.Name)
                        .ToArray();
                var govPosts = m.MemberGovernmentPosts
                    .Where(p => p.EndDate == null)
                    .Select(p => p.GovernmentPost.Name)
                    .ToArray();
                member.GovernmentPosts = string.Join("; ", govPosts);
                var oppPosts = m.MemberOppositionPosts
                            .Where(p => p.EndDate == null)
                            .Select(p => p.OppositionPost.Name)
                            .ToArray();
                member.OppositionPosts = string.Join("; ", oppPosts);
                member.ParliamentaryPosts = string.Join("\n", parliamentaryPosts);
                yield return member;
            }
        }
    }
}
