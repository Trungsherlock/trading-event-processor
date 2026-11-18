namespace TradeProcessor.Core.Validation;

public sealed record ValidationResult(IReadOnlyList<string> Errors)
{
    public bool IsValid => Errors.Count == 0;
}

public interface ITradeValidator
{
    ValidationResult Validate(Trade trade);
}

/// <summary>Runs every rule and collects all errors, so a rejected trade lists everything that was wrong.</summary>
public sealed class TradeValidator(IEnumerable<ITradeRule> rules) : ITradeValidator
{
    private readonly ITradeRule[] _rules = rules.ToArray();

    public ValidationResult Validate(Trade trade) =>
        new(_rules.Select(r => r.Check(trade)).OfType<string>().ToList());
}
