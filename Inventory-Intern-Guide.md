# Inventory Management System — Intern Developer Guide

**For:** Fresh graduates who know Spring Boot basics but are new to microservices  
**Version:** 1.2 | **Date:** May 2026  
**Stack:** Spring Boot 3.x (Java 21) + Angular 21 + PostgreSQL + Redis + Kafka  

---

## Before You Start: What You Should Already Know

- Java 17+ (records, sealed classes, pattern matching)
- Spring Boot basics (controllers, services, repositories, JPA)
- REST API concepts (GET, POST, PUT, DELETE, status codes)
- SQL basics (joins, indexes, transactions)
- Git basics

## What This Document Will Teach You

- What microservices are and why we use them
- How services talk to each other (REST + Kafka events)
- Service discovery, API gateways, centralized config
- JWT authentication and role-based access
- Event-driven architecture
- Docker Compose for local development

---

## Table of Contents

1. [Microservices 101 — The Basics](#1-microservices-101)
2. [Our System — What Are We Building?](#2-what-are-we-building)
3. [Architecture — The Big Picture](#3-architecture)
4. [Infrastructure — The Tools That Support Our Services](#4-infrastructure)
5. [Service 1: Eureka Server (Service Discovery)](#5-eureka-server)
6. [Service 2: Config Server](#6-config-server)
7. [Service 3: API Gateway](#7-api-gateway)
8. [Service 4: User & Auth Service](#8-user--auth-service)
9. [Service 5: Product Service](#9-product-service)
10. [Service 6: Supplier Service](#10-supplier-service)
11. [Service 7: Customer Service](#11-customer-service)
12. [Service 8: Inventory Service](#12-inventory-service)
13. [Service 9: Order Service](#13-order-service)
14. [Service 10: Reporting & Invoice Service](#14-reporting--invoice-service)
15. [Service 11: Notification Service](#15-notification-service)
16. [How Services Communicate](#16-how-services-communicate)
17. [Common Response Format](#17-common-response-format)
18. [Pagination — How We Handle Large Lists](#18-pagination)
19. [Error Handling](#19-error-handling)
20. [Angular Frontend](#20-angular-frontend)
21. [Database Design](#21-database-design)
22. [Security](#22-security)
23. [Running Locally with Docker](#23-running-locally)
24. [Development Timeline](#24-timeline)
25. [All API Endpoints](#25-all-endpoints)
26. [Glossary](#26-glossary)
27. [Your First Week — What to Do](#27-your-first-week--what-to-do-step-by-step)
28. [How to Test Your APIs](#28-how-to-test-your-apis)
29. [Common Mistakes Interns Make](#29-common-mistakes-interns-make-and-how-to-avoid-them)
30. [Recommended Learning Path](#30-recommended-learning-path-books--resources)

---

## 1. Microservices 101

### What is a Monolith?

In college, you probably built apps like this:

```
One big Spring Boot app
├── UserController
├── ProductController
├── OrderController
├── InvoiceController
└── One big database with all tables
```

This is called a **monolith** — everything lives in one application, one JAR file, one database. It works fine for small apps, but problems appear as the app grows:

- One bug in Orders can crash the entire app (including Products, Users, everything)
- You can't scale just the busy part — you scale the whole thing
- 10 developers editing the same codebase = merge conflicts everywhere
- Deploying a tiny fix requires redeploying the entire application

### What are Microservices?

Instead of one big app, we split it into **small, independent applications** that each do ONE thing well:

```
Product Service    → only handles products (its own app, its own database)
Order Service      → only handles orders (its own app, its own database)
Inventory Service  → only handles stock (its own app, its own database)
```

Each service:
- Is a **separate Spring Boot application** (separate JAR, separate port)
- Has its **own database** (Product Service can't directly read Order Service's tables)
- Can be **deployed independently** (fix a bug in Orders without touching Products)
- Can be **scaled independently** (if Orders is busy, run 3 copies of it)
- Communicates with other services via **REST APIs** or **events (Kafka)**

### The Trade-off

| Monolith | Microservices |
|----------|--------------|
| Simple to build | More complex setup |
| One database, easy joins | No cross-service joins |
| One deployment | Many deployments |
| Hard to scale parts | Easy to scale parts |
| One failure = everything down | One failure = only that service down |

**Why are we using microservices?** For learning. This project teaches you the patterns used at companies like Netflix, Amazon, and Flipkart.

### Key Concepts You'll Learn

| Concept | What It Means | Our Tool |
|---------|--------------|----------|
| Service Discovery | "Where is Product Service running?" | Eureka |
| API Gateway | "One door for the frontend to enter" | Spring Cloud Gateway |
| Centralized Config | "All settings in one place" | Config Server (DB + Redis) |
| Event-Driven | "Services notify each other without waiting" | Apache Kafka |
| Circuit Breaker | "If a service is down, fail fast instead of waiting" | Resilience4j |

---

## 2. What Are We Building?

An **Inventory Management System** — the kind of software a shop/warehouse uses to:

1. **Track products** — What do we sell? What's the price? What category?
2. **Track stock** — How many units do we have? When to reorder?
3. **Manage orders** — Purchase orders (buying from suppliers) and Sales orders (selling to customers)
4. **Manage suppliers** — Who do we buy from? At what cost?
5. **Manage customers** — Who do we sell to? How much do they owe us?
6. **Generate invoices** — Bills with GST calculation
7. **Send notifications** — Low stock alerts, order updates
8. **Show reports** — Revenue, stock value, overdue payments

### Real-World Example

Imagine you run an electronics shop:
1. You **add products** (Earphones ₹100, Charger ₹200)
2. You **buy from suppliers** (Purchase Order to Sony for 50 earphones at ₹60 each)
3. Supplier delivers → **stock increases** (50 earphones now in inventory)
4. Customer walks in → **Sales Order** (10 earphones at ₹100 each)
5. You deliver → **stock decreases**, **invoice generated** (₹1000 + GST)
6. Customer pays → **payment recorded**, balance updated
7. Stock drops below 10 → **notification**: "Reorder earphones!"

---

## 3. Architecture

### 3.1 The Big Picture (How Everything Connects)

```mermaid
graph TB
    subgraph Frontend["🖥️ Frontend (What the user sees)"]
        Angular["Angular 21 App<br/>Running on port 4200"]
    end

    subgraph Gateway["🚪 The Single Door"]
        GW["API Gateway :8080<br/>Every request goes through here"]
    end

    subgraph Services["🏢 Business Services (Each is a separate Spring Boot app)"]
        AUTH["Auth Service :8084<br/>Login, Users, Roles"]
        PRD["Product Service :8081<br/>Products, Categories"]
        SUP["Supplier Service :8085<br/>Suppliers"]
        CUS["Customer Service :8089<br/>Customers, Ledger"]
        INV["Inventory Service :8082<br/>Stock, Movements"]
        ORD["Order Service :8083<br/>PO, SO, Approvals"]
        RPI["Report & Invoice :8087<br/>Invoices, Reports"]
        NTF["Notification :8086<br/>Alerts, Emails"]
    end

    subgraph Infra["🔧 Infrastructure (Shared tools)"]
        PG[("PostgreSQL :5432<br/>9 separate databases")]
        RD[("Redis :6379<br/>Caching")]
        KF["Kafka :9092<br/>Event messaging"]
        EU["Eureka :8761<br/>Service registry"]
        CF["Config Server :8888<br/>Settings"]
    end

    Angular -->|"All requests go here"| GW
    GW -->|"Routes to correct service"| AUTH
    GW --> PRD
    GW --> SUP
    GW --> CUS
    GW --> INV
    GW --> ORD
    GW --> RPI
    GW --> NTF

    AUTH --> PG
    PRD --> PG
    SUP --> PG
    CUS --> PG
    INV --> PG
    ORD --> PG
    RPI --> PG
    NTF --> PG

    AUTH --> RD
    PRD --> KF
    INV --> KF
    ORD --> KF
    RPI --> KF
    NTF --> KF
    CUS --> KF
```

### 3.2 How a Request Travels (Step by Step)

Let's say the user clicks "Create Product" in the Angular app:

```
Step 1: Angular sends POST /api/products to port 8080 (Gateway)
Step 2: Gateway checks "Is this user logged in?" (validates JWT token)
Step 3: Gateway asks Eureka "Where is product-service running?"
Step 4: Eureka says "It's at 172.18.0.5:8081"
Step 5: Gateway forwards the request to Product Service
Step 6: Product Service saves to product_db, publishes "product.created" event to Kafka
Step 7: Response travels back: Product Service → Gateway → Angular
Step 8: Meanwhile, Inventory Service picks up the Kafka event and creates a stock record (qty=0)
```

### 3.3 Why Do We Need All These Extra Pieces?

| Component | Without It | With It |
|-----------|-----------|---------|
| **API Gateway** | Frontend needs to know the IP:port of every service (8081, 8082, 8083...) | Frontend only knows one address: `localhost:8080` |
| **Eureka** | If a service moves to a different port, everything breaks | Services register themselves; Gateway finds them automatically |
| **Config Server** | Each service has its own application.yml with hardcoded values | Change a setting in one place, all services pick it up |
| **Kafka** | Service A calls Service B directly; if B is down, A fails too | Service A publishes an event; B processes it when it's ready |
| **Redis** | Every config read hits the database | Frequently-read data served from memory (100x faster) |

---

## 4. Infrastructure

These are the **shared tools** that all services use. Think of them as the "building utilities" (electricity, water, internet) that every apartment in a building shares.

### 4.1 PostgreSQL — The Database

One PostgreSQL server, but **9 separate databases** inside it. Each service gets its own database and cannot access another service's database.

```
Why separate databases?
→ If Product Service has a bug that corrupts data, only product_db is affected
→ Each service can evolve its schema independently
→ No accidental coupling through shared tables
```

**Databases:**
| Database | Owner Service |
|----------|--------------|
| config_db | Config Server |
| auth_db | Auth Service |
| product_db | Product Service |
| supplier_db | Supplier Service |
| customer_db | Customer Service |
| inventory_db | Inventory Service |
| order_db | Order Service |
| report_invoice_db | Reporting & Invoice Service |
| notification_db | Notification Service |

### 4.2 Redis — The Cache

Redis is an **in-memory key-value store**. It's like a HashMap that lives outside your application and is shared across services.

**We use it for:**
- Config Server caches settings (so it doesn't hit PostgreSQL on every request)
- Auth Service stores blacklisted refresh tokens
- API Gateway stores rate-limit counters ("this user made 50 requests in the last minute")

**Why not just use a Java HashMap?**
→ Because each service runs in its own process (its own JVM). A HashMap in Service A is invisible to Service B. Redis is external and shared.

### 4.3 Apache Kafka — The Event Bus

Kafka is a **message broker**. Services publish events ("something happened") and other services consume those events.

**Analogy:** Think of Kafka as a WhatsApp group:
- Order Service posts: "Hey everyone, order #SO-001 was just fulfilled!"
- Inventory Service reads it: "Got it, I'll deduct the stock"
- Invoice Service reads it: "Got it, I'll generate an invoice"
- Notification Service reads it: "Got it, I'll notify the customer"

Nobody waits for anyone else. The message sits in Kafka until each service is ready to process it.

**Key Kafka concepts:**
| Term | Meaning |
|------|---------|
| **Topic** | A named channel (like a WhatsApp group). Example: `order.placed` |
| **Producer** | Service that publishes a message to a topic |
| **Consumer** | Service that reads messages from a topic |
| **Consumer Group** | Ensures each message is processed only once per service |

### 4.4 LocalStack — Fake AWS on Your Laptop

LocalStack simulates AWS services locally. We use:
- **S3** (file storage) — for product images and invoice PDFs
- **SES** (email service) — for sending notification emails

**Why not use real AWS?** It costs money and requires internet. LocalStack is free and runs offline in Docker.

### 4.5 Eureka — Service Discovery

When you run 11 Spring Boot apps, each on a different port, how does the Gateway know where each one is?

**Without Eureka:** You hardcode `product-service: http://localhost:8081` in Gateway's config. If the port changes, you manually update it.

**With Eureka:** Each service says "Hi Eureka, I'm product-service and I'm at port 8081" on startup. Gateway asks Eureka "Where is product-service?" and gets the answer dynamically.

```mermaid
sequenceDiagram
    participant PS as Product Service
    participant EU as Eureka
    participant GW as API Gateway

    PS->>EU: "I'm product-service at 192.168.1.5:8081" (on startup)
    Note over EU: Stores in registry

    GW->>EU: "Where is product-service?"
    EU-->>GW: "It's at 192.168.1.5:8081"
    GW->>PS: Forward the request
```

---

## 5. Eureka Server

**What:** A registry where all services register themselves.  
**Port:** 8761  
**Dashboard:** Open `http://localhost:8761` in browser to see all registered services.

### What You'll Code

This is the simplest service — just one class:

```java
@SpringBootApplication
@EnableEurekaServer  // This annotation does all the magic
public class EurekaServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }
}
```

**application.yml:**
```yaml
server:
  port: 8761
eureka:
  client:
    register-with-eureka: false  # Don't register yourself with yourself
    fetch-registry: false
```

**Dependencies:** `spring-cloud-starter-netflix-eureka-server`

That's it. Eureka is a "set it and forget it" service.

---

## 6. Config Server

**What:** A centralized place to store settings for all services.  
**Port:** 8888  
**Special:** Uses PostgreSQL to store configs + Redis to cache them.

### Why Do We Need This?

Without Config Server, each service has its own `application.yml`:
```
product-service/application.yml  → spring.datasource.url = jdbc:postgresql://...
order-service/application.yml    → spring.datasource.url = jdbc:postgresql://...
inventory-service/application.yml → spring.datasource.url = jdbc:postgresql://...
```

If the database password changes, you update 8 files and restart 8 services.

With Config Server, all settings live in one database table. Change once → all services pick it up.

### How It Works

```mermaid
sequenceDiagram
    participant PS as Product Service
    participant CS as Config Server
    participant RD as Redis
    participant DB as PostgreSQL

    PS->>CS: "Give me my config" (on startup)
    CS->>RD: Check cache
    alt Cache HIT
        RD-->>CS: Return cached config
    else Cache MISS
        CS->>DB: Query config_property table
        DB-->>CS: Return config
        CS->>RD: Store in cache (5 min TTL)
    end
    CS-->>PS: Here's your config
```

### Database Table

```sql
CREATE TABLE config_property (
    id              BIGSERIAL PRIMARY KEY,
    application     VARCHAR(100),  -- 'product-service' or 'application' (shared)
    profile         VARCHAR(50),   -- 'dev' or 'prod'
    prop_key        VARCHAR(255),  -- 'spring.datasource.url'
    prop_value      TEXT,          -- 'jdbc:postgresql://localhost:5432/product_db'
    is_encrypted    BOOLEAN,       -- true for passwords
    description     VARCHAR(500),
    updated_by      VARCHAR(100),
    updated_at      TIMESTAMP
);
```

### API Endpoints

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| GET | `/config/{app}/{profile}` | Services call this on startup to get their config |
| GET | `/admin/config` | Admin UI: see all settings |
| POST | `/admin/config` | Admin UI: add a new setting |
| PUT | `/admin/config/{id}` | Admin UI: change a setting |
| DELETE | `/admin/config/{id}` | Admin UI: remove a setting |
| POST | `/admin/config/refresh/{app}` | Tell a service to reload its config |

---

## 7. API Gateway

**What:** The single entry point for ALL requests from the frontend.  
**Port:** 8080  
**Think of it as:** A security guard + receptionist at a building entrance.

### What Does It Do?

| Job | How |
|-----|-----|
| **Routing** | `/api/products/**` → forward to Product Service |
| **Authentication** | Check JWT token on every request |
| **Rate Limiting** | Block users making too many requests (100/min) |
| **CORS** | Allow Angular (port 4200) to call the API (port 8080) |
| **Circuit Breaker** | If a service is down, return error immediately instead of waiting 30 seconds |
| **Correlation ID** | Add a unique ID to every request for tracing/debugging |

### Route Configuration

```yaml
# "If the URL starts with /api/products, send it to product-service"
spring:
  cloud:
    gateway:
      routes:
        - id: product-service
          uri: lb://product-service    # lb = load-balanced via Eureka
          predicates:
            - Path=/api/products/**, /api/categories/**
          filters:
            - StripPrefix=1            # Remove /api from the path before forwarding
```

`lb://product-service` means: "Ask Eureka where product-service is, then send the request there."

### JWT Filter (The Security Guard)

Every request (except login) must have a valid JWT token:

```
Request comes in:
  Header: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9...

Gateway does:
  1. Extract the token
  2. Verify signature (is it genuine? not tampered?)
  3. Check expiry (is it still valid?)
  4. Extract user info (userId, roles, permissions)
  5. Add user info as headers for downstream service:
     X-User-Id: user-001
     X-User-Roles: ADMIN,WAREHOUSE_MANAGER
     X-User-Permissions: PRODUCTS.VIEW,PRODUCTS.CREATE
  6. Forward request to the target service

If token is invalid → return 401 Unauthorized (request never reaches the service)
```

### Public Routes (No Token Needed)

- `POST /api/auth/login` — you can't require a token to get a token!
- `POST /api/auth/refresh` — uses refresh token, not access token
- `GET /actuator/health` — health check for monitoring

---

## 8. User & Auth Service

**What:** Handles login, users, roles, permissions, and the dynamic menu.  
**Port:** 8084  
**Database:** auth_db

### Why Is This Important?

Every other service trusts the Auth Service. When a user logs in, Auth Service creates a JWT token. That token is like an ID card — it proves who you are and what you're allowed to do.

### Key Concepts

**JWT (JSON Web Token):** A signed string that contains user info. The Gateway validates it on every request.

```
JWT contains:
{
  "sub": "user-uuid-001",        ← who is this?
  "roles": ["ADMIN"],            ← what role?
  "permissions": ["PRODUCTS.VIEW", "ORDERS.CREATE"],  ← what can they do?
  "exp": 1716624900              ← when does this expire? (15 minutes)
}
```

**RBAC (Role-Based Access Control):** Users have Roles. Roles have Permissions. Permissions control what you can do.

```
User "Aman" → has Role "ADMIN" → Role has Permission "PRODUCTS.CREATE"
User "Intern" → has Role "VIEWER" → Role has Permission "PRODUCTS.VIEW" (but NOT CREATE)
```

**Dynamic Menu:** The sidebar menu in Angular is NOT hardcoded. It comes from the backend, filtered by what the user is allowed to see.

### Login Flow (Step by Step)

```mermaid
sequenceDiagram
    participant U as User (Browser)
    participant GW as API Gateway
    participant AS as Auth Service
    participant DB as auth_db

    U->>GW: POST /api/auth/login {username: "aman", password: "Pass@123"}
    GW->>AS: Forward (public route, no JWT needed)
    AS->>DB: Find user by username
    DB-->>AS: User found (password_hash stored)
    AS->>AS: Compare password with BCrypt hash
    AS->>AS: Generate JWT (15 min) + Refresh Token (7 days)
    AS->>DB: Save refresh token (hashed)
    AS-->>U: {accessToken: "eyJ...", refreshToken: "abc...", expiresIn: 900}

    Note over U: User stores tokens in memory

    U->>GW: GET /api/auth/menu (with Bearer token)
    GW->>GW: Validate JWT ✓
    GW->>AS: Forward
    AS->>DB: Get menu items + user's permissions
    AS->>AS: Filter: only return items user has permission for
    AS-->>U: {menu: [{label: "Products", route: "/products"}, ...]}
```

### Database Tables

| Table | What It Stores |
|-------|---------------|
| `users` | Username, email, password hash, status |
| `roles` | ADMIN, WAREHOUSE_MANAGER, PROCUREMENT, ACCOUNTANT, VIEWER |
| `permissions` | PRODUCTS.VIEW, PRODUCTS.CREATE, ORDERS.APPROVE, etc. |
| `user_roles` | Which user has which role |
| `role_permissions` | Which role has which permission |
| `menu_items` | Sidebar menu entries (label, icon, route, required permission) |
| `refresh_tokens` | Active refresh tokens (for logout/revocation) |
| `audit_logs` | Who logged in, when, from where |

### API Endpoints (22)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| POST | `/auth/login` | Login → get JWT tokens |
| POST | `/auth/refresh` | Get new access token using refresh token |
| POST | `/auth/logout` | Invalidate refresh token |
| GET | `/auth/me` | Get current user's profile |
| GET | `/auth/menu` | Get sidebar menu (filtered by permissions) |
| GET | `/auth/permissions` | Get flat list of user's permissions |
| GET/POST/PUT | `/users/**` | CRUD for user management (admin only) |
| GET/POST/PUT | `/roles/**` | CRUD for role management (admin only) |
| GET | `/permissions` | List all available permissions |
| GET/POST/PUT | `/admin/menu-items/**` | Manage menu structure (admin only) |

### Business Rules

- **Password requirements:** Min 8 chars, 1 uppercase, 1 number, 1 special character
- **Account locking:** 5 failed login attempts → account locked for 30 minutes
- **Token expiry:** Access token = 15 minutes, Refresh token = 7 days
- **System roles:** ADMIN and VIEWER cannot be deleted (they're built-in)

---

## 9. Product Service

**What:** Manages the product catalog — what you sell, at what price, in which category.  
**Port:** 8081  
**Database:** product_db

### Think of It As

The "master list" of everything your business deals with. Other services reference products by ID but don't store product details themselves.

### Database Tables

| Table | Fields | Purpose |
|-------|--------|---------|
| `products` | id, sku, name, description, category_id, hsn_code, base_price, status | The main product record |
| `categories` | id, name, parent_id | Hierarchical categories (Electronics → Headphones → Wireless) |
| `product_attributes` | id, product_id, key, value | Flexible specs (color=Red, weight=150g) |
| `product_images` | id, product_id, image_url, is_primary | Photos stored in S3 |

### Key Concepts

**SKU (Stock Keeping Unit):** A unique code for each product. Like a roll number for products. Example: `ELC-00042` (Electronics, item #42).

**HSN Code:** A government-mandated code for GST tax classification. Every product must have one for invoicing.

**Soft Delete:** We never actually DELETE a product from the database. We set `status = DISCONTINUED`. Why? Because old orders and invoices still reference it.

### API Endpoints (12)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| POST | `/products` | Create a new product |
| GET | `/products` | List products (with search, filter, pagination) |
| GET | `/products/{id}` | Get one product's full details |
| PUT | `/products/{id}` | Update a product |
| PATCH | `/products/{id}/status` | Activate or Discontinue |
| POST | `/products/{id}/images` | Upload a product image (goes to S3) |
| POST | `/products/bulk-import` | Upload a CSV to create many products at once |
| GET/POST/PUT | `/categories/**` | Manage product categories |

### Events Published (via Kafka)

When something happens to a product, this service tells others:

| Event | Who Cares | Why |
|-------|-----------|-----|
| `product.created` | Inventory Service | Creates a stock record with quantity = 0 |
| `product.updated` | Reporting Service | Updates its cached product info |
| `product.discontinued` | Inventory Service | Flags stock for clearance |

---

## 10. Supplier Service

**What:** Manages who you buy from and at what price.  
**Port:** 8085  
**Database:** supplier_db

### Real-World Analogy

You sell earphones for ₹100. But you buy them from Sony at ₹60 and from JBL at ₹65. This service tracks:
- Supplier details (Sony, JBL — their address, GSTIN, contact)
- Which supplier provides which product, at what cost, with what delivery time

### Database Tables

| Table | Purpose |
|-------|---------|
| `suppliers` | Supplier master data (name, GSTIN, contact, payment terms) |
| `supplier_products` | Links: "Sony supplies Earphone-X at ₹60, delivers in 5 days" |

### Key Concept: Preferred Supplier

Each product can have multiple suppliers, but one is marked as **preferred**. When stock runs low, the system suggests ordering from the preferred supplier.

```
Product: Wireless Earphone
├── Sony     → ₹60, 5 days delivery, PREFERRED ✓
├── JBL      → ₹65, 3 days delivery
└── boAt     → ₹55, 10 days delivery
```

### API Endpoints (10)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| POST | `/suppliers` | Register a new supplier |
| GET | `/suppliers` | List all suppliers |
| GET | `/suppliers/{id}` | Supplier details |
| PUT | `/suppliers/{id}` | Update supplier info |
| POST | `/suppliers/{id}/products` | "Sony now supplies Earphone-X at ₹60" |
| GET | `/suppliers/for-product/{productId}` | "Who supplies this product?" (ranked) |

---

## 11. Customer Service

**What:** Manages who you sell to and tracks their financial account (how much they owe you).  
**Port:** 8089  
**Database:** customer_db

### The Account Ledger Concept

Every customer has a running balance — like a khata (ledger) at a local shop:

```
Customer: ABC Electronics
┌─────────────────────────────────────────────────────────────────┐
│ Date       │ Description              │ Debit  │ Credit │ Balance│
├─────────────────────────────────────────────────────────────────┤
│ 01-May     │ Opening Balance          │        │        │ ₹0    │
│ 05-May     │ Invoice INV-001 raised   │ ₹5000  │        │ ₹5000 │
│ 10-May     │ Payment received (UPI)   │        │ ₹3000  │ ₹2000 │
│ 15-May     │ Invoice INV-002 raised   │ ₹8000  │        │ ₹10000│
│ 20-May     │ Credit note (return)     │        │ ₹1000  │ ₹9000 │
└─────────────────────────────────────────────────────────────────┘
Current Balance: ₹9000 (customer owes us ₹9000)
```

**DEBIT** = customer owes more (we raised an invoice)  
**CREDIT** = customer owes less (they paid, or we gave a credit note)

### Credit Limit

You can set a limit: "ABC Electronics can owe us max ₹50,000". If their balance exceeds this, the system alerts the sales team.

### API Endpoints (12)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| POST | `/customers` | Create customer |
| GET | `/customers/{id}/account` | See balance, credit limit, last transaction |
| GET | `/customers/{id}/transactions` | Full transaction history |
| POST | `/customers/{id}/transactions` | Manual adjustment (correction) |
| GET | `/customers/{id}/statement` | Statement for a date range (like a bank statement) |
| GET | `/customers/outstanding` | All customers who owe money |

### Events

**Listens to:**
- `invoice.created` → automatically DEBIT the customer
- `invoice.paid` → automatically CREDIT the customer

**Publishes:**
- `customer.credit_exceeded` → Notification Service alerts the team
- `customer.blocked` → Order Service rejects new orders for this customer

---

## 12. Inventory Service

**What:** Tracks how much stock you have, records every movement, and alerts when stock is low.  
**Port:** 8082  
**Database:** inventory_db  
**This is the HEART of the system.**

### Core Concept: Stock Record

For every product, there's one stock record:

```
Product: Wireless Earphone (PRD-042)
┌────────────────────────────────────┐
│ quantity_on_hand:   100            │  ← physically in the warehouse
│ quantity_reserved:   20            │  ← held for confirmed orders (not yet shipped)
│ quantity_available:  80            │  ← what you can actually sell right now
└────────────────────────────────────┘

Formula: available = on_hand - reserved
```

### Stock Movements (The Audit Trail)

Every time stock changes, we record WHY:

| Movement Type | When | Effect |
|--------------|------|--------|
| GOODS_RECEIPT | Supplier delivers goods (PO received) | on_hand ↑ |
| GOODS_ISSUE | Customer order shipped (SO fulfilled) | on_hand ↓ |
| ADJUSTMENT | Manual correction (damage, theft, audit) | on_hand ↑ or ↓ |
| RESERVATION | Sales order confirmed | reserved ↑ (available ↓) |
| RELEASE | Order cancelled or reservation expired | reserved ↓ (available ↑) |

**Why track movements?** Audit trail. If stock doesn't match physical count, you can trace exactly what happened.

### Optimistic Locking (Important Concept!)

**Problem:** Two people try to update stock at the same time.

```
Thread A reads: quantity = 100 (version = 1)
Thread B reads: quantity = 100 (version = 1)
Thread A updates: quantity = 95, version = 2  → SUCCESS ✓
Thread B updates: quantity = 90, version = 2  → FAILS ✗ (expected version 1, found 2)
Thread B retries: re-reads (qty=95, version=2), updates to 85, version=3 → SUCCESS ✓
```

We use JPA's `@Version` annotation for this. It prevents two operations from accidentally overwriting each other.

### Reorder Rules

You can set: "When earphones drop below 20 units, alert me (or auto-create a purchase order)."

```java
ReorderRule:
  product_id: PRD-042
  reorder_point: 20        // alert when available <= 20
  reorder_quantity: 50     // suggest ordering 50 units
  is_auto_reorder: false   // just alert, don't auto-create PO
```

### API Endpoints (15)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| GET | `/inventory` | All stock levels (paginated) |
| GET | `/inventory/{productId}` | Stock for one product |
| GET | `/inventory/check` | "Is there enough stock for these items?" (Order Service calls this) |
| POST | `/inventory/receive` | Goods received from supplier → stock goes UP |
| POST | `/inventory/issue` | Goods shipped to customer → stock goes DOWN |
| POST | `/inventory/adjust` | Manual correction (must provide reason) |
| POST | `/inventory/reserve` | Hold stock for a confirmed order |
| POST | `/inventory/release` | Release held stock (order cancelled) |
| GET | `/inventory/low-stock` | Products below reorder point |
| GET | `/inventory/movements` | Full movement history |

### Events

**Publishes:**
- `stock.updated` → Reporting Service updates dashboards
- `stock.low` → Notification Service alerts warehouse manager
- `stock.reserved` → Order Service knows reservation succeeded

**Listens to:**
- `product.created` → create stock record (qty = 0)
- `order.placed` → reserve stock
- `order.fulfilled` → deduct stock (reservation → actual issue)
- `order.cancelled` → release reservation
- `purchase.order.received` → goods receipt (stock goes up)

---

## 13. Order Service

**What:** Manages Purchase Orders (buying from suppliers) and Sales Orders (selling to customers).  
**Port:** 8083  
**Database:** order_db

### Two Types of Orders

| | Purchase Order (PO) | Sales Order (SO) |
|---|---|---|
| **Direction** | We BUY from supplier | We SELL to customer |
| **Effect on stock** | Stock goes UP (when received) | Stock goes DOWN (when shipped) |
| **Party** | Supplier | Customer |
| **Example** | "Buy 50 earphones from Sony at ₹60 each" | "Sell 10 earphones to ABC Corp at ₹100 each" |

### Order Lifecycle (State Machine)

An order goes through stages. You can't skip stages.

```mermaid
stateDiagram-v2
    direction LR
    state "Sales Order" as SO {
        [*] --> DRAFT: Create
        DRAFT --> CONFIRMED: Confirm (stock reserved)
        DRAFT --> CANCELLED: Cancel
        CONFIRMED --> PICKING: Start picking
        PICKING --> PACKED: Packed
        PACKED --> SHIPPED: Shipped
        SHIPPED --> DELIVERED: Delivered
        DELIVERED --> CLOSED: Close
    }
```

```mermaid
stateDiagram-v2
    direction LR
    state "Purchase Order" as PO {
        [*] --> DRAFT: Create
        DRAFT --> SUBMITTED: Submit
        SUBMITTED --> APPROVED: Approve
        SUBMITTED --> REJECTED: Reject
        APPROVED --> SENT_TO_SUPPLIER: Send
        SENT_TO_SUPPLIER --> PARTIALLY_RECEIVED: Partial delivery
        SENT_TO_SUPPLIER --> RECEIVED: Full delivery
        PARTIALLY_RECEIVED --> RECEIVED: Rest delivered
        RECEIVED --> CLOSED: Close
    }
```

**Invalid transitions are blocked.** You can't go from DRAFT directly to SHIPPED — that makes no sense.

### Approval Workflow

Purchase Orders above a configurable amount (e.g., ₹50,000) require manager approval before being sent to the supplier.

### API Endpoints (12)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| POST | `/orders` | Create a new order (PO or SO) |
| GET | `/orders` | List orders (filter by type, status, date) |
| GET | `/orders/{id}` | Order detail with items and status history |
| PATCH | `/orders/{id}/status` | Move to next status (e.g., CONFIRMED → PICKING) |
| POST | `/orders/{id}/items` | Add a product to the order |
| POST | `/orders/{id}/receive` | Record goods receipt (for PO) |
| PATCH | `/orders/{id}/approve` | Approve a PO |
| PATCH | `/orders/{id}/reject` | Reject a PO (reason required) |

### How Sales Order Confirmation Works

```mermaid
sequenceDiagram
    participant U as User
    participant OS as Order Service
    participant IS as Inventory Service

    U->>OS: "Confirm this sales order"
    OS->>IS: "Do you have 10 earphones available?" (REST call)
    IS-->>OS: "Yes, 80 available"
    OS->>OS: Save order as CONFIRMED
    OS->>OS: Publish "order.placed" to Kafka
    OS-->>U: "Order confirmed!"

    Note over IS: Later, Inventory consumes "order.placed"
    IS->>IS: Reserve 10 earphones (available: 80→70)
```

---

## 14. Reporting & Invoice Service

**What:** Generates invoices (with GST), tracks payments, manages discounts, and produces business reports.  
**Port:** 8087  
**Database:** report_invoice_db

### Why Is Invoicing Combined with Reporting?

Invoices are the primary source of financial data. Reports like "revenue this month" or "overdue payments" come directly from invoice data. Keeping them together avoids cross-service calls.

### GST Tax Calculation (India-Specific)

```
If seller and buyer are in the SAME state:
  CGST = 9% (Central GST)
  SGST = 9% (State GST)
  Total tax = 18%

If seller and buyer are in DIFFERENT states:
  IGST = 18% (Integrated GST)
  Total tax = 18%

The rate (18%, 12%, 5%) depends on the product's HSN code.
```

**Example Invoice Line:**
```
Product: Wireless Earphone (HSN: 8518)
Quantity: 10
Unit Price: ₹100
Discount: 10%
Taxable Amount: 10 × 100 × (1 - 10/100) = ₹900
CGST (9%): ₹81
SGST (9%): ₹81
Line Total: ₹900 + ₹81 + ₹81 = ₹1062
```

### Invoice Lifecycle

```mermaid
stateDiagram-v2
    direction LR
    [*] --> DRAFT: Create
    DRAFT --> ISSUED: Issue
    DRAFT --> CANCELLED: Cancel
    ISSUED --> SENT: Send to customer
    SENT --> PARTIALLY_PAID: Partial payment
    SENT --> OVERDUE: Past due date
    PARTIALLY_PAID --> PAID: Full payment
    OVERDUE --> PAID: Late payment
    PAID --> CLOSED: Close
    ISSUED --> VOID: Void (with credit note)
```

**Key rule:** Once an invoice is ISSUED, you CANNOT edit it. If there's a mistake, you issue a Credit Note (a "reverse invoice").

### Discount Rules Engine

You can create rules like:
- "10% off all Electronics" (category discount)
- "VIP customers get 15% off everything" (customer group discount)
- "₹500 off orders above ₹10,000" (order-level discount)

### Auto-Invoice Generation

When a sales order is fulfilled (delivered), this service automatically creates an invoice:

```
Event: "order.fulfilled" arrives from Kafka
→ Service creates Invoice in DRAFT status
→ Copies product details, quantities, prices from the order
→ Calculates GST based on customer's state vs our state
→ Publishes "invoice.created" event
→ Customer Service debits the customer's account
```

### API Endpoints (28)

**Invoices:** Create, list, update, record payments, generate PDF, check overdue  
**Tax Rates:** Manage GST rates (5%, 12%, 18%, 28%)  
**Discounts:** Create rules, calculate applicable discounts  
**Customer Groups:** VIP, Wholesale, Retail (for group discounts)  
**Reports:** Dashboard KPIs, stock valuation, aging report, tax summary

---

## 15. Notification Service

**What:** Sends alerts to users when important things happen.  
**Port:** 8086  
**Database:** notification_db

### How It Works

This service doesn't decide WHEN to notify — other services do that by publishing events. This service just:
1. Listens to Kafka events
2. Looks up the notification template
3. Fills in the placeholders
4. Delivers via the user's preferred channel

### Channels

| Channel | How | When |
|---------|-----|------|
| **In-App** | Stored in DB, pushed via WebSocket | Always (default) |
| **Email** | Sent via LocalStack SES | If user opted in |

### Example Flow

```
1. Inventory Service publishes: stock.low {productName: "Earphone", currentQty: 5, reorderPoint: 20}
2. Notification Service consumes the event
3. Finds template: "⚠️ Low Stock: {{productName}} — Current: {{currentQty}}, Reorder at: {{reorderPoint}}"
4. Renders: "⚠️ Low Stock: Earphone — Current: 5, Reorder at: 20"
5. Finds target users: all users with INVENTORY.VIEW permission
6. For each user: check their preference (in-app? email? both?)
7. Deliver accordingly
```

### API Endpoints (10)

| Method | Endpoint | What It Does |
|--------|----------|-------------|
| GET | `/notifications` | Your notification inbox |
| GET | `/notifications/unread-count` | Number for the bell badge |
| PATCH | `/notifications/{id}/read` | Mark one as read |
| PATCH | `/notifications/read-all` | Mark all as read |
| PUT | `/notifications/preferences` | "I want email for low-stock, but not for orders" |

---

## 16. How Services Communicate

### Two Ways Services Talk

| Method | When to Use | Example |
|--------|------------|---------|
| **REST (Synchronous)** | You need an answer RIGHT NOW | Order Service asks Inventory: "Is stock available?" |
| **Kafka (Asynchronous)** | You're just informing others, don't need a response | Order Service tells everyone: "Order was fulfilled" |

### REST = Phone Call

```
Order Service: "Hey Inventory, do you have 10 earphones?"
Inventory Service: "Yes, 80 available."
Order Service: "Great, I'll confirm the order."
```

Both services must be running. If Inventory is down, Order gets an error.

### Kafka = WhatsApp Group Message

```
Order Service posts to Kafka: "Order #SO-001 was fulfilled"

Inventory reads it (whenever ready): "I'll deduct stock"
Invoice Service reads it (whenever ready): "I'll generate an invoice"
Notification reads it (whenever ready): "I'll notify the customer"
```

Nobody waits for anyone. If Notification Service is down for 5 minutes, the message waits in Kafka. When it comes back up, it processes the message.

### Complete Event Map

```mermaid
graph TD
    subgraph Producers["Who publishes events"]
        PRD[Product Service]
        INV[Inventory Service]
        ORD[Order Service]
        RPI[Report & Invoice]
        CUS[Customer Service]
    end

    subgraph Kafka["Apache Kafka (Message Broker)"]
        T1[product.created]
        T2[stock.updated]
        T3[stock.low]
        T4[order.placed]
        T5[order.fulfilled]
        T6[order.cancelled]
        T7[invoice.created]
        T8[invoice.overdue]
        T9[invoice.paid]
        T10[customer.blocked]
    end

    subgraph Consumers["Who listens"]
        INV2[Inventory Service]
        ORD2[Order Service]
        RPI2[Report & Invoice]
        NTF[Notification Service]
        CUS2[Customer Service]
        SUP[Supplier Service]
    end

    PRD --> T1
    INV --> T2
    INV --> T3
    ORD --> T4
    ORD --> T5
    ORD --> T6
    RPI --> T7
    RPI --> T8
    RPI --> T9
    CUS --> T10

    T1 --> INV2
    T2 --> RPI2
    T3 --> NTF
    T3 --> SUP
    T4 --> INV2
    T5 --> INV2
    T5 --> RPI2
    T6 --> INV2
    T7 --> CUS2
    T7 --> NTF
    T8 --> NTF
    T9 --> CUS2
    T9 --> NTF
    T10 --> ORD2
```

### Event Payload (What a Kafka Message Looks Like)

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "eventType": "order.fulfilled",
  "timestamp": "2026-05-25T10:30:00Z",
  "source": "order-service",
  "correlationId": "request-trace-id",
  "payload": {
    "orderId": "uuid-123",
    "orderNumber": "SO-2526-00042",
    "items": [
      {"productId": "uuid-456", "quantity": 10, "unitPrice": 100.00}
    ],
    "customerId": "uuid-789",
    "totalAmount": 1180.00
  }
}
```

### What Happens If a Consumer Fails?

```
Normal: Event → Consumer processes it → Done ✓
Failure: Event → Consumer fails → Retry 3 times → Still fails → Send to Dead Letter Topic (DLT)

DLT = a special topic where failed messages go. An admin can inspect and replay them later.
```

---

## 17. Common Response Format

**Every API in every service returns the same JSON structure.** This makes the Angular frontend's job much easier.

### Success Response

```json
{
  "success": true,
  "status": 200,
  "message": "Product created successfully",
  "data": {
    "id": "uuid-001",
    "sku": "ELC-00042",
    "name": "Wireless Earphone",
    "basePrice": 100.00
  },
  "timestamp": "2026-05-25T10:30:00.123Z",
  "path": "/api/products",
  "correlationId": "abc-123"
}
```

### Error Response

```json
{
  "success": false,
  "status": 404,
  "message": "Product not found",
  "error": {
    "code": "PRD_001",
    "details": "No product exists with ID: uuid-999",
    "suggestions": ["Check the product ID and try again"]
  },
  "timestamp": "2026-05-25T10:30:00.123Z",
  "path": "/api/products/uuid-999",
  "correlationId": "abc-123"
}
```

### Validation Error (Multiple Fields Wrong)

```json
{
  "success": false,
  "status": 400,
  "message": "Validation failed",
  "error": {
    "code": "COM_001",
    "details": "3 validation errors",
    "fieldErrors": [
      {"field": "name", "message": "Name is required", "rejectedValue": null},
      {"field": "basePrice", "message": "Price must be > 0", "rejectedValue": -10},
      {"field": "hsnCode", "message": "Must be 4-8 digits", "rejectedValue": "AB"}
    ]
  }
}
```

### Java Implementation

```java
// Every controller returns this wrapper
public class ApiResponse<T> {
    private boolean success;
    private int status;
    private String message;
    private T data;           // null on error
    private ErrorDetails error; // null on success
    private String timestamp;
    private String path;
    private String correlationId;
}
```

---

## 18. Pagination

When you have 10,000 products, you don't return all of them at once. You return 20 at a time (a "page").

### Request

```
GET /api/products?page=0&size=20&sort=name,asc&search=earphone

page = 0      → first page (0-indexed, like arrays)
size = 20     → 20 items per page
sort = name,asc → sort by name, ascending
search = earphone → filter by keyword
```

### Response

```json
{
  "success": true,
  "data": {
    "content": [
      {"id": "uuid-1", "name": "Bluetooth Earphone", "price": 150},
      {"id": "uuid-2", "name": "Wireless Earphone", "price": 100}
    ],
    "pagination": {
      "page": 0,
      "size": 20,
      "totalElements": 156,
      "totalPages": 8,
      "first": true,
      "last": false,
      "hasNext": true,
      "hasPrevious": false
    }
  }
}
```

### Rules

- Max page size: 100 (even if you ask for 500, you get 100)
- Empty results return `content: []` with `totalElements: 0` (NOT a 404 error)
- Default sort varies by endpoint (products: by creation date, customers: by name)

---

## 19. Error Handling

### Error Code Format

Each service has a prefix:

```
COM_001 = Common error #1 (shared across all services)
AUTH_001 = Auth Service error #1
PRD_001 = Product Service error #1
INV_001 = Inventory Service error #1
ORD_001 = Order Service error #1
```

### HTTP Status Codes (When to Use Which)

| Code | Meaning | Example |
|------|---------|---------|
| **200** | OK | GET request succeeded |
| **201** | Created | POST created a new resource |
| **204** | No Content | DELETE succeeded (nothing to return) |
| **400** | Bad Request | Validation failed, malformed JSON |
| **401** | Unauthorized | No token, or token expired |
| **403** | Forbidden | Valid token, but you don't have permission |
| **404** | Not Found | Product with this ID doesn't exist |
| **409** | Conflict | Trying to create a product with an existing SKU |
| **422** | Unprocessable | Business rule violated (e.g., "can't ship before packing") |
| **429** | Too Many Requests | Rate limit exceeded |
| **500** | Server Error | Bug in our code (should never happen intentionally) |

### Common Mistakes

```
❌ Returning 200 with an error message in the body
✓ Returning 404 when something isn't found

❌ Returning 500 for "user not found"
✓ Returning 404 for "user not found" (it's not a server error, it's expected)

❌ Returning 400 for "insufficient stock"
✓ Returning 422 for "insufficient stock" (request is valid, but business rule says no)
```

### Key Exceptions Per Service

| Service | Code | HTTP | When |
|---------|------|------|------|
| Auth | AUTH_001 | 401 | Wrong password |
| Auth | AUTH_002 | 401 | Account locked |
| Product | PRD_001 | 404 | Product not found |
| Product | PRD_002 | 409 | Duplicate SKU |
| Inventory | INV_002 | 422 | Not enough stock |
| Inventory | INV_003 | 409 | Concurrent update conflict |
| Order | ORD_002 | 422 | Invalid status transition (e.g., DRAFT → SHIPPED) |
| Order | ORD_004 | 422 | Stock not available for confirmation |
| Invoice | RPI_003 | 422 | Can't edit issued invoice |
| Invoice | RPI_005 | 422 | Payment exceeds balance due |
| Customer | CUS_004 | 422 | Credit limit exceeded |

---

## 20. Angular Frontend

The frontend is built with **Angular 21** (latest). You don't need to be an Angular expert — just understand the structure.

### How It Connects to Backend

```
Angular app (port 4200) → sends all API calls to → API Gateway (port 8080)
Angular NEVER talks directly to individual services.
```

### Feature Modules (Each is a "mini-app")

```
src/app/features/
├── auth/          → Login, forgot password
├── dashboard/     → KPI cards, charts
├── products/      → Product CRUD, categories
├── suppliers/     → Supplier management
├── customers/     → Customer management, account view
├── inventory/     → Stock levels, adjustments
├── orders/        → Create/manage PO and SO
├── invoices/      → Invoice management, payments
├── reports/       → Stock valuation, aging, tax summary
└── settings/      → User management, roles, system config
```

### Dynamic Menu

The sidebar menu is NOT hardcoded in Angular. On login:
1. Angular calls `GET /api/auth/menu`
2. Backend returns only the menu items this user has permission to see
3. Angular renders the menu from this response

**Admin sees:** Dashboard, Products, Inventory, Orders, Suppliers, Customers, Invoices, Reports, Settings  
**Viewer sees:** Dashboard, Products (read-only), Orders (read-only), Reports

### Permission Checks in Angular

```typescript
// In a component template:
@if (permissionService.hasPermission('ORDERS.APPROVE')) {
  <button>Approve Order</button>
}

// In route guards:
canActivate: [permissionGuard('INVENTORY.VIEW')]
// → If user doesn't have this permission, they can't navigate to this page
```

---

## 21. Database Design

### The Golden Rule: Database Per Service

```
❌ WRONG: All services share one database
   → Product Service accidentally reads Order tables
   → Changing a column in products table breaks Order Service

✓ RIGHT: Each service has its own database
   → Product Service can only access product_db
   → Order Service can only access order_db
   → They communicate via REST/Kafka, NOT via shared tables
```

### How Do Services Reference Each Other's Data?

They store the **UUID** of the other service's entity, but NOT a foreign key:

```sql
-- In order_db (Order Service's database)
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    party_id UUID,          -- This is a customer_id, but NO foreign key to customer_db
    party_name VARCHAR(200) -- We COPY the name here (denormalization)
);
```

**Why copy the name?** Because we can't JOIN across databases. If we need to display "Order #001 for ABC Corp", we need the name stored locally.

### Total Tables: 36

| Database | Tables | Count |
|----------|--------|-------|
| config_db | config_property, config_change_log | 2 |
| auth_db | users, roles, permissions, user_roles, role_permissions, menu_items, refresh_tokens, audit_logs | 8 |
| product_db | products, categories, product_attributes, product_images | 4 |
| supplier_db | suppliers, supplier_products | 2 |
| customer_db | customers, account_transactions | 2 |
| inventory_db | stock, stock_movements, reorder_rules | 3 |
| order_db | orders, order_items, order_status_history | 3 |
| report_invoice_db | invoices, invoice_line_items, tax_rates, payment_records, credit_notes, discount_rules, customer_groups, customer_group_members, daily_stock_snapshots | 9 |
| notification_db | notification_templates, notification_preferences, notification_logs | 3 |

---

## 22. Security

### The 5 Layers

```
Layer 1: API Gateway      → "Do you have a valid ID card?" (JWT check)
Layer 2: Service Level    → "Are you allowed to do this?" (permission check)
Layer 3: Data Level       → "Can you see this data?" (scoped queries)
Layer 4: Database Level   → Each service has its own DB credentials
Layer 5: Network Level    → Only Gateway is exposed; services are internal
```

### JWT Tokens Explained

```
Access Token (short-lived: 15 minutes)
├── Contains: userId, roles, permissions
├── Used for: Every API call
├── Stored in: Angular memory (signal) — NOT localStorage!
└── If expired: Use refresh token to get a new one

Refresh Token (long-lived: 7 days)
├── Contains: Just a random string
├── Used for: Getting a new access token
├── Stored in: Angular memory + hashed in auth_db
└── If expired: User must login again
```

**Why NOT localStorage?** Any JavaScript on the page (including malicious scripts from XSS attacks) can read localStorage. Memory is safer.

### Password Storage

```
User enters: "MyPassword@123"
We store: "$2a$12$LJ3m4ks9..." (BCrypt hash, strength 12)

We NEVER store the actual password. We can only verify: "Does this input produce the same hash?"
```

---

## 23. Running Locally

### What You Need Installed

- Docker Desktop (for running all containers)
- Java 21 (for building services)
- Node.js 20+ (for Angular)
- Your IDE (IntelliJ IDEA recommended for Spring Boot)

### Starting Everything

```bash
# Step 1: Start infrastructure (databases, cache, messaging)
docker-compose up -d postgresql redis zookeeper kafka localstack

# Step 2: Start platform services
docker-compose up -d eureka-server config-server api-gateway

# Step 3: Start business services
docker-compose up -d auth-service product-service supplier-service customer-service
docker-compose up -d inventory-service order-service reporting-invoice-service notification-service

# Step 4: Start frontend
docker-compose up -d angular-frontend
```

### Ports to Remember

| Service | Port | URL |
|---------|------|-----|
| Angular Frontend | 4200 | http://localhost:4200 |
| API Gateway | 8080 | http://localhost:8080/api/... |
| Eureka Dashboard | 8761 | http://localhost:8761 |
| PostgreSQL | 5432 | Connect via pgAdmin or DBeaver |
| Redis | 6379 | Connect via redis-cli |
| Kafka | 9092 | — |

### Checking If Everything Is Running

```bash
docker-compose ps          # See all container statuses
docker-compose logs -f auth-service  # Watch auth-service logs in real-time
```

### Common Issues

| Problem | Solution |
|---------|----------|
| Service can't connect to PostgreSQL | Wait for PostgreSQL health check to pass (takes ~10 seconds) |
| Eureka shows 0 instances | Services take 30 seconds to register after startup |
| Kafka consumer not receiving events | Check consumer group ID matches; check topic exists |
| 401 on every request | Your JWT expired (15 min). Login again or check refresh logic |

---

## 24. Timeline

**Total: ~35 weeks (8.5 months) for a solo developer**

| Phase | What You Build | Weeks | What You Learn |
|-------|---------------|-------|---------------|
| 0. Infrastructure | Docker Compose, all containers | 1-3 | Docker, Kafka, Redis basics |
| 1. Config + Gateway | Config Server, API Gateway | 4-5 | Spring Cloud, Eureka, JWT filters |
| 2. Auth + Angular | Login, RBAC, dynamic menu, Angular shell | 6-9 | JWT, security, Angular basics |
| 3. Products | Product CRUD, categories, S3 images | 10-12 | REST APIs, file upload, events |
| 4. Suppliers | Supplier CRUD, product linking | 12.5-14 | Service-to-service references |
| 5. Customers | Customer CRUD, account ledger | 14.5-16 | Financial logic, event consumers |
| 6. Inventory | Stock, movements, reservations, reorder | 16.5-20 | Optimistic locking, schedulers |
| 7. Orders | PO/SO lifecycle, approvals | 20.5-24.5 | State machines, sync REST calls |
| 8. Invoicing & Reports | GST invoices, payments, discounts, reports | 25-30.5 | Tax calculation, CQRS, PDF |
| 9. Notifications | Templates, Kafka consumers, WebSocket | 31-33 | WebSocket, email delivery |
| 10. Polish | End-to-end testing, bug fixes | 33.5-35 | Integration testing |

---

## 25. All Endpoints

**Total: 129 API endpoints across 8 business services + Config Server**

| Service | Count | Key Endpoints |
|---------|-------|--------------|
| Config Server | 8 | GET/POST/PUT/DELETE `/admin/config` |
| Auth Service | 22 | `/auth/login`, `/auth/menu`, `/users`, `/roles` |
| Product Service | 12 | `/products`, `/categories`, `/products/bulk-import` |
| Supplier Service | 10 | `/suppliers`, `/suppliers/{id}/products` |
| Customer Service | 12 | `/customers`, `/customers/{id}/account`, `/customers/{id}/transactions` |
| Inventory Service | 15 | `/inventory`, `/inventory/receive`, `/inventory/adjust`, `/inventory/reserve` |
| Order Service | 12 | `/orders`, `/orders/{id}/status`, `/orders/{id}/approve` |
| Report & Invoice | 28 | `/invoices`, `/tax-rates`, `/discounts`, `/reports/dashboard` |
| Notification | 10 | `/notifications`, `/notifications/preferences` |

---

## 26. Glossary

| Term | Meaning |
|------|---------|
| **Microservice** | A small, independent application that does one thing well |
| **API Gateway** | Single entry point that routes requests to the correct service |
| **Service Discovery** | Mechanism for services to find each other (Eureka) |
| **JWT** | JSON Web Token — a signed token proving who you are |
| **Kafka** | Message broker for async communication between services |
| **Topic** | A named channel in Kafka (like a WhatsApp group) |
| **Producer** | Service that publishes messages to Kafka |
| **Consumer** | Service that reads messages from Kafka |
| **Event-Driven** | Architecture where services react to events instead of being called directly |
| **RBAC** | Role-Based Access Control — permissions assigned via roles |
| **Optimistic Locking** | Preventing concurrent updates using a version number |
| **Circuit Breaker** | Pattern that fails fast when a downstream service is unhealthy |
| **CQRS** | Command Query Responsibility Segregation — separate read/write models |
| **Denormalization** | Copying data to avoid cross-service calls (e.g., storing customer_name in orders) |
| **Dead Letter Queue** | Where failed messages go after max retries |
| **Saga** | Pattern for managing transactions across multiple services |
| **Idempotent** | An operation that produces the same result even if called multiple times |
| **HSN Code** | Harmonized System Nomenclature — product classification for GST |
| **GSTIN** | GST Identification Number — 15-character tax ID for businesses |
| **SKU** | Stock Keeping Unit — unique product identifier |
| **PO** | Purchase Order — order to buy from a supplier |
| **SO** | Sales Order — order to sell to a customer |
| **Soft Delete** | Marking a record as inactive instead of actually deleting it |
| **Seed Data** | Initial data loaded into the database (admin user, default roles, etc.) |
| **Health Check** | Endpoint (`/actuator/health`) that reports if a service is running correctly |
| **Correlation ID** | A unique ID that follows a request across all services (for debugging) |
| **LocalStack** | Tool that simulates AWS services locally (S3, SES) |
| **S3** | AWS Simple Storage Service — file/object storage |
| **SES** | AWS Simple Email Service — sending emails |
| **BCrypt** | Password hashing algorithm (one-way, with salt) |
| **TTL** | Time To Live — how long a cached value stays valid |

---

## 27. Your First Week — What to Do (Step by Step)

Most interns feel overwhelmed looking at 11 services. Here's your exact path:

### Day 1-2: Understand the Basics

```
□ Read Sections 1-4 of this guide (Microservices 101, Architecture, Infrastructure)
□ Install Docker Desktop, Java 21, IntelliJ IDEA
□ Run: docker-compose up -d postgresql redis zookeeper kafka
□ Connect to PostgreSQL using DBeaver/pgAdmin (host: localhost, port: 5432, user: admin)
□ Open http://localhost:8761 — this is Eureka (it'll be empty for now, that's OK)
```

### Day 3-4: Run Your First Service

```
□ Open eureka-server project in IntelliJ
□ Run it. Open http://localhost:8761 — you should see the Eureka dashboard
□ Open auth-service project
□ Run it. Check Eureka — "AUTH-SERVICE" should appear in the registry
□ Use Postman: POST http://localhost:8084/auth/login with {"username": "admin", "password": "Admin@123"}
□ You should get a JWT token back. Copy it.
□ Use Postman: GET http://localhost:8084/auth/me with Header: Authorization: Bearer <your-token>
□ You should see the admin user's profile. Congratulations — you just authenticated!
```

### Day 5: Understand the Gateway

```
□ Start api-gateway
□ Try the same login through the gateway: POST http://localhost:8080/api/auth/login
□ Notice: same request, but through port 8080 (gateway) instead of 8084 (direct)
□ Try accessing a protected endpoint WITHOUT a token → you should get 401
□ Try WITH the token → you should get the response
□ Now you understand: Gateway = security guard + router
```

### Week 2: Build Something

```
□ Pick ONE service (start with Product Service — it's the simplest business service)
□ Read its section in this guide
□ Look at the code: Entity → Repository → Service → Controller
□ Create a product using Postman
□ Check the database — see the row you just created
□ Check Kafka — see the "product.created" event (use Kafdrop at localhost:9000)
□ Check Inventory Service's database — a stock record with qty=0 should appear automatically
□ You just witnessed event-driven architecture in action!
```

### The Golden Rule for Interns

```
Don't try to understand ALL 11 services at once.
Understand ONE service deeply (how it starts, how it connects to DB, how it publishes events).
Then the others follow the same pattern.
```

---

## 28. How to Test Your APIs

### 28.1 Using Postman (Recommended for Beginners)

**Setup:**
1. Download Postman (free)
2. Create a collection called "Inventory Management"
3. Create folders: Auth, Products, Orders, etc.

**Testing Login:**
```
Method: POST
URL: http://localhost:8080/api/auth/login
Body (JSON):
{
  "username": "admin",
  "password": "Admin@123"
}

Expected: 200 OK with accessToken in response
```

**Testing a Protected Endpoint:**
```
Method: GET
URL: http://localhost:8080/api/products
Headers:
  Authorization: Bearer eyJhbGciOiJIUzI1NiJ9... (paste your token here)

Expected: 200 OK with list of products
```

**Pro tip:** In Postman, set up an "Environment Variable" for the token. Use a Pre-request Script to auto-login and store the token. This saves you from manually copying tokens every 15 minutes.

### 28.2 Using curl (Command Line)

```bash
# Login
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"Admin@123"}'

# Use the token
curl http://localhost:8080/api/products \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9..."
```

### 28.3 Testing Kafka Events

After creating a product, check if the event was published:

**Option A: Kafdrop UI** (if running)
- Open http://localhost:9000
- Click on topic "product.created"
- See the message with product details

**Option B: Kafka CLI**
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic product.created \
  --from-beginning
```

### 28.4 Testing Checklist (For Every Endpoint You Build)

```
□ Happy path works (valid input → expected output)
□ Missing required field → 400 with field error
□ Invalid ID → 404
□ No auth token → 401
□ Wrong permission → 403
□ Duplicate entry → 409
□ Business rule violation → 422
□ Pagination works (page=0, page=1, size=5)
□ Search/filter works
□ Kafka event published (check Kafdrop)
```

---

## 29. Common Mistakes Interns Make (And How to Avoid Them)

| Mistake | Why It's Wrong | What to Do Instead |
|---------|---------------|-------------------|
| Calling another service's database directly | Breaks service independence. If they change their schema, your code breaks. | Use REST calls or Kafka events to communicate. |
| Putting business logic in the Controller | Controllers should only handle HTTP (parse request, return response). | Put logic in the Service layer. |
| Not handling errors | Unhandled exceptions return ugly 500 errors with stack traces. | Use `@ExceptionHandler` and return proper `ApiResponse`. |
| Hardcoding URLs | `http://localhost:8081/products` breaks when the port changes. | Use Eureka service names: `http://product-service/products`. |
| Using `System.out.println` for debugging | Doesn't show in structured logs, can't filter, no timestamp. | Use `log.info()`, `log.error()` with SLF4J. |
| Committing secrets to Git | Passwords in `application.yml` get pushed to the repo. | Use Config Server for secrets. Never commit passwords. |
| Not using transactions | Two DB operations can partially succeed (data corruption). | Use `@Transactional` on service methods that do multiple writes. |
| Ignoring Kafka consumer failures | If processing fails silently, data gets lost. | Always log errors. Use idempotency checks. Handle exceptions. |
| Testing only the happy path | Real users send empty strings, negative numbers, SQL injection. | Test edge cases: null, empty, negative, very long strings, special characters. |
| Not reading logs when something fails | Staring at the error in Postman won't help. | Always check `docker-compose logs -f <service-name>` for the full stack trace. |

---

## 30. Recommended Learning Path (Books & Resources)

### Must-Read (In This Order)

| # | Resource | What You'll Learn |
|---|----------|------------------|
| 1 | **Spring Boot in Action** (Craig Walls) | Deep Spring Boot understanding |
| 2 | **Building Microservices** (Sam Newman) | Why and how to split services |
| 3 | **Designing Data-Intensive Applications** (Martin Kleppmann) | Databases, Kafka, distributed systems |
| 4 | **Spring Microservices in Action** (John Carnell) | Spring Cloud (Eureka, Gateway, Config) |

### Free Resources

| Resource | URL | What |
|----------|-----|------|
| Baeldung | baeldung.com | Spring Boot tutorials (best on the internet) |
| Spring Docs | spring.io/guides | Official guides |
| Kafka Docs | kafka.apache.org/quickstart | Kafka basics |
| Docker Docs | docs.docker.com/get-started | Docker fundamentals |

### YouTube Channels

- **Java Brains** — Spring Boot + Microservices (beginner-friendly)
- **Amigoscode** — Spring Boot + Docker
- **TechPrimers** — System design + Spring Cloud

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────┐
│              INVENTORY MANAGEMENT SYSTEM                  │
├─────────────────────────────────────────────────────────┤
│ Services: 11 (8 business + 3 platform)                  │
│ Endpoints: 129 REST APIs                                │
│ Events: 18 Kafka event types                            │
│ Tables: 36 across 9 databases                           │
│ Containers: 17 Docker containers                        │
│ RAM needed: ~5 GB (16 GB machine recommended)           │
│ Timeline: ~35 weeks solo                                │
├─────────────────────────────────────────────────────────┤
│ Frontend: http://localhost:4200                          │
│ Gateway:  http://localhost:8080                          │
│ Eureka:   http://localhost:8761                          │
│ Config:   http://localhost:8888                          │
│ Kafdrop:  http://localhost:9000                          │
│ PostgreSQL: localhost:5432                               │
└─────────────────────────────────────────────────────────┘
```

---

*End of Intern Guide — v1.2*
