using TradeProcessor.Core.Validation.Rules;
using TradeProcessor.UnitTests.TestData;

namespace TradeProcessor.UnitTests.Validation;

public class AccountIdRequiredRuleTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]   // missing from the JSON
    public void Rejects_missing_account_id(string? accountId)
    {
        var trade = TradeBuilder.Valid() with { AccountId = accountId! };
        Assert.NotNull(new AccountIdRequiredRule().Check(trade));
    }

    [Fact]
    public void Accepts_account_id() =>
        Assert.Null(new AccountIdRequiredRule().Check(TradeBuilder.Valid()));
}
