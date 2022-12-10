package io.mykidong.iceberg.example.dataapi.util;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class JwtUtils {

    private static final String DEFAULT_SECRET = "default-secret-chango-rksflja";
    public static final long JWT_TOKEN_VALIDITY = 5 * 60 * 60;

    public static String getAccessToken(HttpServletRequest req)
    {
        String accessToken = null;

        String bearerToken = req.getHeader("Authorization");

        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            accessToken = bearerToken.substring(7, bearerToken.length());
        }
        else
        {
            return null;
        }

        return accessToken;
    }


    public static String getUsernameFromToken(String token) {
        return getClaimFromToken(token, Claims::getSubject);
    }

    public static Date getExpirationDateFromToken(String token) {
        return getClaimFromToken(token, Claims::getExpiration);
    }

    public static <T> T getClaimFromToken(String token, Function<Claims, T> claimsResolver) {
        final Claims claims = getAllClaimsFromToken(token);
        return claimsResolver.apply(claims);
    }

    public static Claims getAllClaimsFromToken(String token) {
        return Jwts.parser().setSigningKey(DEFAULT_SECRET).parseClaimsJws(token).getBody();
    }

    public static Boolean isTokenExpired(String token) {
        try {
            final Date expiration = getExpirationDateFromToken(token);
            return expiration.before(new Date());
        } catch (Exception e) {
            return true;
        }
    }

    public static String generateToken(Map<String, Object> claims, String userName) {
        if(claims == null) {
            claims = new HashMap<>();
        }
        return doGenerateToken(claims, userName);
    }

    private static String doGenerateToken(Map<String, Object> claims, String subject) {
        return Jwts.builder().setClaims(claims).setSubject(subject).setIssuedAt(new Date(System.currentTimeMillis()))
                .setExpiration(new Date(System.currentTimeMillis() + JWT_TOKEN_VALIDITY * 1000))
                .signWith(SignatureAlgorithm.HS512, DEFAULT_SECRET).compact();
    }

    public static Boolean validateToken(String token, String userName) {
        return (getUsernameFromToken(token).equals(userName) && !isTokenExpired(token));
    }
}
