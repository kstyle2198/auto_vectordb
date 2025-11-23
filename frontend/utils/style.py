HOVERING_EFFECT = """
    <style>
    /* 링크 기본 밑줄 제거 + 텍스트 색상 상속 */
    .clickable-box-wrapper {
        text-decoration: none !important;
        color: inherit;
    }

    /* hover-box에 padding 적용 */
    .hover-box {
        padding: 20px;
    }

    /* 텍스트 기본 색상 회색 */
    .hover-box h1, .hover-box p {
        color: #888888;
        transition: color 0.3s ease;
    }

    /* Hover 시 텍스트 색상 파란색 */
    .st-emotion-cache-1vo6xi6:hover .hover-box h1,
    .st-emotion-cache-1vo6xi6:hover .hover-box p {
        color: #007bff;
    }

    /* container hover 효과 */
    .st-emotion-cache-1vo6xi6 {
        transition: all 0.35s ease;
        border-radius: 16px;
    }
    .st-emotion-cache-1vo6xi6:hover {
        background: rgba(255, 255, 255, 0.25);
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.25);
        backdrop-filter: blur(12px) saturate(150%);
        -webkit-backdrop-filter: blur(12px) saturate(150%);
        transform: translateY(-6px);
    }
    </style>
    """