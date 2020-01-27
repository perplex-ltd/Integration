using Perplex.Integration.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Perplex.ArtsCouncil.MPImport
{

    public enum House
    {
        Commons, Lords
    }

    public class Member : Row
    {

        public int Id { 
            get => (int)this["Id"]; 
            set => this["Id"] = value;
        }
        public string FirstName
        {
            get => (string)this["FirstName"];
            set => this["FirstName"] = value;
        }
        public string MiddleNames
        {
            get => (string)this["MiddleNames"];
            set => this["MiddleNames"] = value;
        }
        public string LastName
        {
            get => (string)this["LastName"];
            set => this["LastName"] = value;
        }
        public string AddressAs
        {
            get => (string)this["AddressAs"];
            set => this["AddressAs"] = value;
        }
        public string Party
        {
            get => (string)this["Party"];
            set => this["Party"] = value;
        }
        public bool IsActiveMember
        {
            get => (bool)this["IsActiveMember"];
            set => this["IsActiveMember"] = value;
        }
        public DateTime? StartDate
        {
            get => (DateTime?)this["StartDate"];
            set => this["StartDate"] = value;
        }

        public DateTime? DateOfBirth
        {
            get => (DateTime?)this["DateOfBirth"];
            set => this["DateOfBirth"] = value;
        }
        public DateTime? DateOfDeath
        {
            get => (DateTime?)this["DateOfDeath"];
            set => this["DateOfDeath"] = value;
        }
        public string House
        {
            get => (string)this["House"];
            set => this["House"] = value;
        }
        public string Constituency {
            get => (string)this["Constituency"];
            set => this["Constituency"] = value;
        }
        public string TypeOfPeer {
            get => (string)this["TypeOfPeer"];
            set => this["TypeOfPeer"] = value;
        }
        public DateTime? EndDate
        {
            get => (DateTime?)this["EndDate"];
            set => this["EndDate"] = value;
        }
        public string EndReason {
            get => (string)this["EndReason"];
            set => this["EndReason"] = value;
        }
        public string EmailAddress1 
        {
            get => (string)this["EmailAddress1"];
            set => this["EmailAddress1"] = value;
        }
        public string EmailAddress2
        {
            get => (string)this["EmailAddress2"];
            set => this["EmailAddress2"] = value;
        }

        public string EmailAddress3
        {
            get => (string)this["EmailAddress3"];
            set => this["EmailAddress3"] = value;
        }

        public string ParliamentaryPosts
        {
            get => (string)this["ParliamentaryPosts"];
            set => this["ParliamentaryPosts"] = value;
        }
        public string GovernmentPosts
        {
            get => (string)this["GovernmentPosts"];
            set => this["GovernmentPosts"] = value;
        }
        public string OppositionPosts
        {
            get => (string)this["OppositionPosts"];
            set => this["OppositionPosts"] = value;
        }

    }
}
