# OpenTelemetry Code Analysis & Instrumentation

Analyze code and provide specific instrumentation recommendations optimized for Honeycomb, with a preference for using OpenTelemetry tracing if possible.

Support direct instructions, like "instrument this file" or "add tracing to this function."

## Rules to follow

Ignore metrics code, if it exists, for now.

Focus on enhancing any existing logs, then suggest span instrumentation if requested.

If there are no existing logging calls, suggest using OpenTelemetry spans instead, unless explicitly asked to add logs.

## Logging Enhancements

If code has logging, recommend improvements to:

1. Add proper log levels for different types of operations:
   ```
   # Instead of:
   print("Processing order")
   logger.info(f"Order {id} status: {status}")

   # Better as:
   logger.info("Starting order processing", {"app.order_id": id})
   logger.error("Order processing failed", {"app.order_id": id, "app.error": str(e)})
   ```

2. Convert print statements to structured logs:
   ```
   # Instead of:
   print(f"Processing order {id} for customer {customer}")

   # Better as:
   logger.info("Processing order", {
       "app.order_id": id,
       "app.customer_id": customer.id,
       "app.items_count": len(items)
   })
   ```

3. Consolidate related logs into single, rich events:
   ```
   # Instead of multiple logs:
   logger.info(f"Processing order {id}")
   items = process_order(id)
   logger.info(f"Found {len(items)} items")
   discount = apply_discount(items)
   logger.info(f"Applied discount: {discount}")

   # Better as one structured log:
   logger.info("Processing order", {
       "app.order_id": id,
       "app.items_count": len(items),
       "app.discount_applied": discount,
       "app.customer_tier": customer.tier
   })
   ```

4. Capture high-cardinality data and data useful for debugging from function parameters in structured fields, such as:
   - `app.user_id`
   - `app.request_id`
   - `app.order_id`, `app.product_id`, etc.
   - Operation parameters
   - State information

In particular, especially focus on consolidating logs that can be combined into a single, rich event.

## Span Instrumentation (If Requested)

If instrumenting iwth spans, recommend instrumentation that:

1. Adds important high-cardinality data, request context, and data useful for debugging from function parameters to the current span:
   ```
   current_span.set_attributes({
     "app.customer_id": request.customer_id,
     "app.order_type": request.type,
     "app.items_count": len(request.items)
   })
   ```

2. Creates spans for meaningful operations:
   ```
   with span("create_order") as order_span:
       order = create_order(request)
       order_span.set_attributes({
           "app.order_id": order.id,
           "app.total_amount": order.total
       })
   ```

3. Handles errors properly:
   ```
   try:
       # operation code
   catch Error as e:
       span.set_attributes({
           "error": true,
           "error.type": type(e),
           "error.message": str(e)
       })
       raise
   ```

## Additional considerations

Always use well-defined, specific, and namespaced keys for structured logs and span attributes.

Consider deeply if clarification is needed on:

- The purpose or context of specific code sections
- Which operations are most important to instrument
- Whether to focus on logging improvements or span creation, especially if both are present
- The meaning of domain-specific terms or variables

Ask for more information before providing recommendations if necessary.