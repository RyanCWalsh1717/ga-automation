"""
Variance Comment Generator for GA Automation Pipeline
======================================================
Generates narrative explanations for material budget variances using:
  1. GL transaction detail behind each flagged variance (data layer)
  2. Claude API call for polished narrative (optional, requires API key)

Falls back to data-driven drafts if the API key is not configured or
the API call fails.
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional


# ââ Data-driven draft generation âââââââââââââââââââââââââââââ

def _build_variance_context(variance: dict, gl_data, budget_data=None) -> dict:
    """
    Build rich context for a single variance by pulling GL transactions
    behind the flagged account.

    Args:
        variance: Dict with account_code, account_name, ptd_actual, ptd_budget,
                  variance, variance_pct
        gl_data: Parsed GL data with accounts and transactions
        budget_data: Optional budget comparison data

    Returns:
        Dict with variance info + supporting GL transaction detail
    """
    acct_code = variance.get('account_code', '')
    context = {
        'account_code': acct_code,
        'account_name': variance.get('account_name', ''),
        'ptd_actual': variance.get('ptd_actual', 0),
        'ptd_budget': variance.get('ptd_budget', 0),
        'variance_amount': variance.get('variance', 0),
        'variance_pct': variance.get('variance_pct', 0),
        'direction': 'over budget' if variance.get('variance', 0) > 0 else 'under budget',
        'transactions': [],
        'vendor_summary': {},
        'transaction_count': 0,
    }

    # Pull GL transactions for this account
    if gl_data and hasattr(gl_data, 'accounts'):
        for acct in gl_data.accounts:
            if acct.account_code == acct_code:
                context['gl_beginning_balance'] = acct.beginning_balance
                context['gl_ending_balance'] = acct.ending_balance
                context['gl_net_change'] = acct.net_change
                context['gl_total_debits'] = acct.total_debits
                context['gl_total_credits'] = acct.total_credits

                if hasattr(acct, 'transactions'):
                    for txn in acct.transactions:
                        txn_dict = {
                            'date': txn.date.strftime('%m/%d/%Y') if txn.date else '',
                            'description': txn.description or '',
                            'control': txn.control or '',
                            'reference': txn.reference or '',
                            'debit': txn.debit,
                            'credit': txn.credit,
                            'net': txn.debit - txn.credit,
                        }
                        context['transactions'].append(txn_dict)

                        # Build vendor/payee summary from description
                        desc = (txn.description or '').strip()
                        if desc:
                            # Use first meaningful word as vendor proxy
                            vendor_key = desc[:40]
                            if vendor_key not in context['vendor_summary']:
                                context['vendor_summary'][vendor_key] = {
                                    'total': 0, 'count': 0, 'descriptions': []
                                }
                            context['vendor_summary'][vendor_key]['total'] += txn.debit - txn.credit
                            context['vendor_summary'][vendor_key]['count'] += 1

                    context['transaction_count'] = len(acct.transactions)
                break

    return context


def generate_data_driven_comment(context: dict) -> str:
    """
    Generate a factual, data-driven variance comment from GL detail.
    No API call â purely mechanical.
    """
    acct = context['account_name']
    var_amt = context['variance_amount']
    var_pct = context['variance_pct']
    direction = context['direction']
    txn_count = context['transaction_count']

    comment = f"{acct} is ${abs(var_amt):,.0f} ({abs(var_pct):.0f}%) {direction}."

    if txn_count == 0:
        comment += " No GL transactions found for this period."
        return comment

    comment += f" {txn_count} transaction(s) in the period."

    # Top drivers by amount
    vendors = context.get('vendor_summary', {})
    if vendors:
        sorted_vendors = sorted(vendors.items(), key=lambda x: abs(x[1]['total']), reverse=True)
        top = sorted_vendors[:3]
        drivers = []
        for desc, info in top:
            drivers.append(f"{desc} (${abs(info['total']):,.0f}, {info['count']} txn)")
        comment += " Key drivers: " + "; ".join(drivers) + "."

    return comment


# ââ Claude API narrative generation ââââââââââââââââââââââââââ

def _build_api_prompt(contexts: List[dict], period: str, property_name: str) -> str:
    """Build the prompt for Claude API to generate variance narratives."""

    variance_details = []
    for ctx in contexts:
        detail = f"""
Account: {ctx['account_code']} â {ctx['account_name']}
  Actual: ${ctx['ptd_actual']:,.2f}  |  Budget: ${ctx['ptd_budget']:,.2f}
  Variance: ${ctx['variance_amount']:+,.2f} ({ctx['variance_pct']:+.1f}%)
  GL Transactions ({ctx['transaction_count']}):"""

        for txn in ctx['transactions'][:10]:  # Limit to 10 transactions per account
            net = txn['net']
            detail += f"\n    {txn['date']}  {txn['description'][:50]}  Control: {txn['control']}  ${net:+,.2f}"

        if ctx['transaction_count'] > 10:
            detail += f"\n    ... and {ctx['transaction_count'] - 10} more transactions"

        variance_details.append(detail)

    prompt = f"""You are a CRE accounting analyst writing variance commentary for a monthly close package.
Property: {property_name}
Period: {period}

Generate a concise 1-2 sentence narrative explanation for each material budget variance below.
Focus on the WHY â what drove the variance based on the GL transaction detail provided.
Use professional accounting language suitable for an institutional investor review.
Do NOT speculate beyond what the data shows. If the cause is unclear from the transactions,
say "requires further investigation" or "timing difference pending verification."

Format your response as a JSON array of objects with keys "account_code" and "comment".

Variances to explain:
{"".join(variance_details)}
"""
    return prompt


def generate_api_comments(contexts: List[dict], period: str = '',
                           property_name: str = '',
                           api_key: str = None) -> Dict[str, str]:
    """
    Call Claude API to generate narrative variance comments.

    Args:
        contexts: List of variance context dicts from _build_variance_context()
        period: Accounting period
        property_name: Property name
        api_key: Anthropic API key

    Returns:
        Dict mapping account_code -> narrative comment string.
        Falls back to data-driven comments on any failure.
    """
    if not api_key:
        # Fallback to data-driven
        return {ctx['account_code']: generate_data_driven_comment(ctx) for ctx in contexts}

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        prompt = _build_api_prompt(contexts, period, property_name)

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse response
        response_text = message.content[0].text

        # Extract JSON from response (handle markdown code blocks)
        json_text = response_text
        if '```json' in json_text:
            json_text = json_text.split('```json')[1].split('```')[0]
        elif '```' in json_text:
            json_text = json_text.split('```')[1].split('```')[0]

        comments_list = json.loads(json_text.strip())

        result = {}
        for item in comments_list:
            code = item.get('account_code', '')
            comment = item.get('comment', '')
            if code and comment:
                result[code] = comment

        # Fill in any missing accounts with data-driven fallback
        for ctx in contexts:
            if ctx['account_code'] not in result:
                result[ctx['account_code']] = generate_data_driven_comment(ctx)

        return result

    except ImportError:
        # anthropic package not installed â fall back
        return {ctx['account_code']: generate_data_driven_comment(ctx) for ctx in contexts}
    except Exception as e:
        # Any API error â fall back with note
        result = {}
        for ctx in contexts:
            comment = generate_data_driven_comment(ctx)
            result[ctx['account_code']] = f"[API unavailable] {comment}"
        return result


# ââ Main entry point âââââââââââââââââââââââââââââââââââââââââ

def generate_variance_comments(engine_result, api_key: str = None) -> List[dict]:
    """
    Generate variance comments for all material budget variances.

    Args:
        engine_result: EngineResult from pipeline run
        api_key: Optional Anthropic API key for narrative generation

    Returns:
        List of dicts with keys: account_code, account_name, variance_amount,
        variance_pct, comment, method ('api' or 'data-driven')
    """
    gl_data = engine_result.parsed.get('gl')
    budget_data = engine_result.parsed.get('budget_comparison')
    variances = engine_result.budget_variances or []

    if not variances:
        return []

    # Build context for each variance
    contexts = []
    for var in variances:
        ctx = _build_variance_context(var, gl_data, budget_data)
        contexts.append(ctx)

    # Generate comments
    method = 'data-driven'
    if api_key:
        comments_map = generate_api_comments(
            contexts,
            period=engine_result.period or '',
            property_name=engine_result.property_name or '',
            api_key=api_key,
        )
        # Check if API was actually used (no "[API unavailable]" prefix)
        sample = next(iter(comments_map.values()), '')
        if not sample.startswith('[API unavailable]'):
            method = 'api'
    else:
        comments_map = {ctx['account_code']: generate_data_driven_comment(ctx) for ctx in contexts}

    # Build output
    results = []
    for var in variances:
        code = var.get('account_code', '')
        results.append({
            'account_code': code,
            'account_name': var.get('account_name', ''),
            'ptd_actual': var.get('ptd_actual', 0),
            'ptd_budget': var.get('ptd_budget', 0),
            'variance_amount': var.get('variance', 0),
            'variance_pct': var.get('variance_pct', 0),
            'comment': comments_map.get(code, ''),
            'method': method,
        })

    return results
