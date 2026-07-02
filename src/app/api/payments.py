"""Payment endpoints."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.deps import SessionDep, require_api_key
from app.schemas import PaymentAcceptedResponse, PaymentCreateRequest, PaymentResponse
from app.services import payments as payments_service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a payment for asynchronous processing",
)
async def create_payment(
    body: PaymentCreateRequest,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    session: SessionDep,
    response: Response,
) -> PaymentAcceptedResponse:
    """Accept a payment and enqueue it via the transactional outbox.

    A repeated request with the same ``Idempotency-Key`` and body returns the
    already-created payment (marked with the ``Idempotency-Replayed`` header);
    the same key with a different body yields ``409 Conflict``.
    """
    payment, replayed = await payments_service.create_payment(session, body, idempotency_key)
    if replayed:
        response.headers["Idempotency-Replayed"] = "true"
    return PaymentAcceptedResponse(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", summary="Get payment details")
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentResponse:
    """Return the current state of a payment."""
    payment = await payments_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentResponse.model_validate(payment)
