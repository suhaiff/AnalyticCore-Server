const axios = require('axios');

class BrevoService {
    constructor() {
        this.apiKey = process.env.BREVO_API_KEY;
        this.senderEmail = process.env.BREVO_SENDER_EMAIL || 'noreply@insightai.com';
        this.senderName = process.env.BREVO_SENDER_NAME || 'InsightAI';
        this.apiUrl = 'https://api.brevo.com/v3/smtp/email';

        if (!this.apiKey) {
            console.warn('⚠️  BREVO_API_KEY not set. Email sending will not work.');
        } else {
            console.log('✓ Brevo email service initialized');
        }
    }

    async sendEmail(toEmail, toName, subject, htmlContent) {
        if (!this.apiKey) {
            console.error('❌ Cannot send email: BREVO_API_KEY not configured');
            return false;
        }

        try {
            const response = await axios.post(this.apiUrl, {
                sender: { name: this.senderName, email: this.senderEmail },
                to: [{ email: toEmail, name: toName || toEmail }],
                subject,
                htmlContent
            }, {
                headers: {
                    'accept': 'application/json',
                    'api-key': this.apiKey,
                    'content-type': 'application/json'
                }
            });

            console.log(`✅ Email sent to ${toEmail}: ${subject}`);
            return true;
        } catch (error) {
            console.error(`❌ Failed to send email to ${toEmail}:`, error.response?.data || error.message);
            return false;
        }
    }

    async sendTemporaryPasswordEmail(toEmail, toName, tempPassword) {
        const subject = '🔐 Your InsightAI Account — Temporary Password';
        const htmlContent = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#0f172a;font-family:'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
    <div style="max-width:520px;margin:40px auto;background:linear-gradient(135deg,#1e293b 0%,#0f172a 100%);border-radius:24px;overflow:hidden;border:1px solid rgba(99,102,241,0.2);box-shadow:0 25px 50px -12px rgba(0,0,0,0.5);">
        <!-- Header -->
        <div style="background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%);padding:40px 32px;text-align:center;">
            <div style="width:56px;height:56px;background:rgba(255,255,255,0.2);border-radius:16px;display:inline-flex;align-items:center;justify-content:center;margin-bottom:16px;">
                <span style="font-size:28px;">🔐</span>
            </div>
            <h1 style="color:#ffffff;font-size:24px;font-weight:800;margin:0;letter-spacing:-0.5px;">Welcome to InsightAI</h1>
            <p style="color:rgba(255,255,255,0.8);font-size:14px;margin:8px 0 0;">Your account has been created successfully</p>
        </div>
        <!-- Body -->
        <div style="padding:32px;">
            <p style="color:#cbd5e1;font-size:15px;line-height:1.6;margin:0 0 24px;">
                Hi <strong style="color:#e2e8f0;">${toName || 'there'}</strong>,
            </p>
            <p style="color:#cbd5e1;font-size:15px;line-height:1.6;margin:0 0 24px;">
                Your InsightAI account is ready! Use the temporary password below to log in for the first time. You'll be asked to set a new password after logging in.
            </p>
            <!-- Password Box -->
            <div style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.3);border-radius:16px;padding:24px;text-align:center;margin:0 0 24px;">
                <p style="color:#94a3b8;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:2px;margin:0 0 12px;">Your Temporary Password</p>
                <p style="color:#e2e8f0;font-size:24px;font-weight:800;font-family:'Courier New',monospace;letter-spacing:3px;margin:0;background:rgba(15,23,42,0.5);padding:12px 20px;border-radius:12px;display:inline-block;">${tempPassword}</p>
            </div>
            <div style="background:rgba(245,158,11,0.1);border:1px solid rgba(245,158,11,0.2);border-radius:12px;padding:16px;margin:0 0 24px;">
                <p style="color:#fbbf24;font-size:13px;margin:0;line-height:1.5;">
                    ⚠️ <strong>Important:</strong> This is a one-time password. Please change it immediately after your first login.
                </p>
            </div>
            <p style="color:#64748b;font-size:13px;line-height:1.6;margin:0;">
                If you didn't request this account, please ignore this email.
            </p>
        </div>
        <!-- Footer -->
        <div style="border-top:1px solid rgba(99,102,241,0.15);padding:20px 32px;text-align:center;">
            <p style="color:#475569;font-size:12px;margin:0;">© ${new Date().getFullYear()} InsightAI — Powered by AnalyticCore</p>
        </div>
    </div>
</body>
</html>`;
        return this.sendEmail(toEmail, toName, subject, htmlContent);
    }

    async sendOtpEmail(toEmail, toName, otp) {
        const subject = '🔑 InsightAI — Password Reset OTP';
        const htmlContent = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#0f172a;font-family:'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
    <div style="max-width:520px;margin:40px auto;background:linear-gradient(135deg,#1e293b 0%,#0f172a 100%);border-radius:24px;overflow:hidden;border:1px solid rgba(99,102,241,0.2);box-shadow:0 25px 50px -12px rgba(0,0,0,0.5);">
        <!-- Header -->
        <div style="background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%);padding:40px 32px;text-align:center;">
            <div style="width:56px;height:56px;background:rgba(255,255,255,0.2);border-radius:16px;display:inline-flex;align-items:center;justify-content:center;margin-bottom:16px;">
                <span style="font-size:28px;">🔑</span>
            </div>
            <h1 style="color:#ffffff;font-size:24px;font-weight:800;margin:0;letter-spacing:-0.5px;">Password Reset</h1>
            <p style="color:rgba(255,255,255,0.8);font-size:14px;margin:8px 0 0;">Use this OTP to reset your password</p>
        </div>
        <!-- Body -->
        <div style="padding:32px;">
            <p style="color:#cbd5e1;font-size:15px;line-height:1.6;margin:0 0 24px;">
                Hi <strong style="color:#e2e8f0;">${toName || 'there'}</strong>,
            </p>
            <p style="color:#cbd5e1;font-size:15px;line-height:1.6;margin:0 0 24px;">
                We received a request to reset your password. Enter the OTP below to proceed:
            </p>
            <!-- OTP Box -->
            <div style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.3);border-radius:16px;padding:24px;text-align:center;margin:0 0 24px;">
                <p style="color:#94a3b8;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:2px;margin:0 0 12px;">Your Verification Code</p>
                <p style="color:#e2e8f0;font-size:36px;font-weight:800;font-family:'Courier New',monospace;letter-spacing:8px;margin:0;background:rgba(15,23,42,0.5);padding:16px 24px;border-radius:12px;display:inline-block;">${otp}</p>
            </div>
            <div style="background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);border-radius:12px;padding:16px;margin:0 0 24px;">
                <p style="color:#f87171;font-size:13px;margin:0;line-height:1.5;">
                    ⏰ <strong>This code expires in 10 minutes.</strong> Do not share it with anyone.
                </p>
            </div>
            <p style="color:#64748b;font-size:13px;line-height:1.6;margin:0;">
                If you didn't request a password reset, you can safely ignore this email.
            </p>
        </div>
        <!-- Footer -->
        <div style="border-top:1px solid rgba(99,102,241,0.15);padding:20px 32px;text-align:center;">
            <p style="color:#475569;font-size:12px;margin:0;">© ${new Date().getFullYear()} InsightAI — Powered by AnalyticCore</p>
        </div>
    </div>
</body>
</html>`;
        return this.sendEmail(toEmail, toName, subject, htmlContent);
    }
}

module.exports = new BrevoService();
