/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.Subject;
import org.opensearch.identity.noop.NoopSubject;
import org.opensearch.identity.noop.NoopTokenManager;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.BearerAuthToken;
import org.opensearch.identity.tokens.OnBehalfOfClaims;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Optional;

import org.passay.CharacterCharacteristicsRule;
import org.passay.CharacterRule;
import org.passay.EnglishCharacterData;
import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.PasswordValidator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AuthTokenHandlerTests extends OpenSearchTestCase {

    private ShiroTokenManager shiroAuthTokenHandler;

    @Before
    public void testSetup() {
        shiroAuthTokenHandler = new ShiroTokenManager();
    }

    public void testShouldExtractBasicAuthTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic YWRtaW46YWRtaW4="); // admin:admin
        assertEquals(authToken.asAuthHeaderValue(), "YWRtaW46YWRtaW4=");

        final AuthenticationToken translatedToken = shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("admin"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("admin"));
    }

    public void testShouldExtractBasicAuthTokenSuccessfully_twoSemiColonPassword() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA=="); // test:te:st

        final AuthenticationToken translatedToken = shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertThat(translatedToken, is(instanceOf(UsernamePasswordToken.class)));

        final UsernamePasswordToken usernamePasswordToken = (UsernamePasswordToken) translatedToken;
        assertThat(usernamePasswordToken, notNullValue());
        assertThat(usernamePasswordToken.getUsername(), equalTo("test"));
        assertThat(new String(usernamePasswordToken.getPassword()), equalTo("te:st"));
    }

    public void testShouldReturnNullWhenExtractingNullToken() {
        final Optional<AuthenticationToken> translatedToken = shiroAuthTokenHandler.translateAuthToken(null);

        assertThat(translatedToken.isEmpty(), is(true));
    }

    public void testShouldRevokeTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assertTrue(authToken.toString().equals("Basic auth token with user=test, password=te:st"));
        shiroAuthTokenHandler.revokeToken(authToken);
        assert (authToken.toString().equals("Basic auth token with user=, password="));
    }

    public void testShouldResetTokenSuccessfully() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        assertTrue(authToken.toString().equals("Basic auth token with user=test, password=te:st"));
        shiroAuthTokenHandler.resetToken(authToken);
        assert (authToken.toString().equals("Basic auth token with user=, password="));
    }

    public void testShouldFailWhenRevokeToken() {
        final BearerAuthToken bearerAuthToken = new BearerAuthToken("header.payload.signature");
        assert (bearerAuthToken.getTokenIdentifier().equals("Bearer"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> shiroAuthTokenHandler.revokeToken(bearerAuthToken));
    }

    public void testShouldFailGetTokenInfo() {
        final BearerAuthToken bearerAuthToken = new BearerAuthToken("header.payload.signature");
        assert (bearerAuthToken.getTokenIdentifier().equals("Bearer"));
        assertThrows(UnsupportedAuthenticationToken.class, () -> shiroAuthTokenHandler.getTokenInfo(bearerAuthToken));
    }

    public void testShoudPassMapLookupWithToken() {
        final BasicAuthToken authToken = new BasicAuthToken("Basic dGVzdDp0ZTpzdA==");
        shiroAuthTokenHandler.getShiroTokenPasswordMap().put(authToken, "te:st");
        assertTrue(authToken.getPassword().equals(shiroAuthTokenHandler.getShiroTokenPasswordMap().get(authToken)));
    }

    public void testShouldPassThroughResetToken() {
        final BearerAuthToken bearerAuthToken = new BearerAuthToken("header.payload.signature");
        shiroAuthTokenHandler.resetToken(bearerAuthToken);
    }

    public void testVerifyBearerTokenObject() {
        BearerAuthToken testGoodToken = new BearerAuthToken("header.payload.signature");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new BearerAuthToken("asddfhadfasdfad"));
        assert (exception.getMessage().contains("Illegally formed bearer authorization token "));
        assertEquals(testGoodToken.getCompleteToken(), "header.payload.signature");
        assertEquals(testGoodToken.getTokenIdentifier(), "Bearer");
        assertEquals(testGoodToken.getHeader(), "header");
        assertEquals(testGoodToken.getPayload(), "payload");
        assertEquals(testGoodToken.getSignature(), "signature");
        assertEquals(testGoodToken.toString(), "Bearer auth token with header=header, payload=payload, signature=signature");
        assertEquals(testGoodToken.asAuthHeaderValue(), "header.payload.signature");
    }

    public void testGeneratedPasswordContents() {
        String password = shiroAuthTokenHandler.generatePassword();
        PasswordData data = new PasswordData(password);

        LengthRule lengthRule = new LengthRule(8, 16);

        CharacterCharacteristicsRule characteristicsRule = new CharacterCharacteristicsRule();

        // Define M (3 in this case)
        characteristicsRule.setNumberOfCharacteristics(3);

        // Define elements of N (upper, lower, digit, symbol)
        characteristicsRule.getRules().add(new CharacterRule(EnglishCharacterData.UpperCase, 1));
        characteristicsRule.getRules().add(new CharacterRule(EnglishCharacterData.LowerCase, 1));
        characteristicsRule.getRules().add(new CharacterRule(EnglishCharacterData.Digit, 1));
        characteristicsRule.getRules().add(new CharacterRule(EnglishCharacterData.Special, 1));

        PasswordValidator validator = new PasswordValidator(lengthRule, characteristicsRule);
        validator.validate(data);
    }

    public void testIssueOnBehalfOfTokenFromClaims() {
        Subject subject = new NoopSubject();
        OnBehalfOfClaims claims = new OnBehalfOfClaims("test", "test");
        BasicAuthToken authToken = (BasicAuthToken) shiroAuthTokenHandler.issueOnBehalfOfToken(subject, claims);
        assertTrue(authToken instanceof BasicAuthToken);
        UsernamePasswordToken translatedToken = (UsernamePasswordToken) shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertEquals(authToken.getPassword(), new String(translatedToken.getPassword()));
        assertTrue(shiroAuthTokenHandler.getShiroTokenPasswordMap().containsKey(authToken));
        assertEquals(shiroAuthTokenHandler.getShiroTokenPasswordMap().get(authToken), new String(translatedToken.getPassword()));
    }

    public void testTokenNoopIssuance() {
        NoopTokenManager tokenManager = new NoopTokenManager();
        OnBehalfOfClaims claims = new OnBehalfOfClaims("test", "test");
        Subject subject = new NoopSubject();
        AuthToken token = tokenManager.issueOnBehalfOfToken(subject, claims);
        assertTrue(token instanceof AuthToken);
        AuthToken serviceAccountToken = tokenManager.issueServiceAccountToken("test");
        assertTrue(serviceAccountToken instanceof AuthToken);
        assertEquals(serviceAccountToken.asAuthHeaderValue(), "noopToken");
    }

    public void testShouldSucceedIssueServiceAccountToken() {
        String audience = "testExtensionName";
        BasicAuthToken authToken = (BasicAuthToken) shiroAuthTokenHandler.issueServiceAccountToken(audience);
        assertTrue(authToken instanceof BasicAuthToken);
        UsernamePasswordToken translatedToken = (UsernamePasswordToken) shiroAuthTokenHandler.translateAuthToken(authToken).get();
        assertEquals(authToken.getPassword(), new String(translatedToken.getPassword()));
        assertTrue(shiroAuthTokenHandler.getShiroTokenPasswordMap().containsKey(authToken));
        assertEquals(shiroAuthTokenHandler.getShiroTokenPasswordMap().get(authToken), new String(translatedToken.getPassword()));
    }
}
